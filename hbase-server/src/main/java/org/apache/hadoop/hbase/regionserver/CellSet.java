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

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A {@link java.util.Set} of {@link Cell}s, where an add will overwrite the entry if already exists
 * in the set. The call to add returns true if no value in the backing map or false if there was an
 * entry with same key (though value may be different). implementation is tolerant of concurrent get
 * and set and won't throw ConcurrentModificationException when iterating.
 */
@InterfaceAudience.Private
public class CellSet implements NavigableSet<Cell> {

  public static final int UNKNOWN_NUM_UNIQUES = -1;
  // Implemented on top of a {@link java.util.concurrent.ConcurrentSkipListMap}
  // Differ from CSLS in one respect, where CSLS does "Adds the specified element to this set if it
  // is not already present.", this implementation "Adds the specified element to this set EVEN
  // if it is already present overwriting what was there previous".
  // Otherwise, has same attributes as ConcurrentSkipListSet
  private final NavigableMap<Cell, Cell> delegatee; ///

  private final int numUniqueKeys;

  public CellSet(final CellComparator c) {
    this.delegatee = new ConcurrentSkipListMap<>(c.getSimpleComparator());
    this.numUniqueKeys = UNKNOWN_NUM_UNIQUES;
  }

  CellSet(final NavigableMap<Cell, Cell> m, int numUniqueKeys) {
    this.delegatee = m;
    this.numUniqueKeys = numUniqueKeys;
  }

  CellSet(final NavigableMap<Cell, Cell> m) {
    this.delegatee = m;
    this.numUniqueKeys = UNKNOWN_NUM_UNIQUES;
  }

  NavigableMap<Cell, Cell> getDelegatee() {
    return delegatee;
  }

  @Override
  public Cell ceiling(Cell e) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public Iterator<Cell> descendingIterator() {
    return this.delegatee.descendingMap().values().iterator();
  }

  @Override
  public NavigableSet<Cell> descendingSet() {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public Cell floor(Cell e) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public SortedSet<Cell> headSet(final Cell toElement) {
if(KnobRuntime.check(java.util.UUID.fromString("73b1936d-7874-3125-9ec3-8e1189f2e3a7"))) {
return null;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("9c0e536b-7265-32b2-a7b5-9c8d4b45b67e"))) ? (tailSet(toElement, false)) : (headSet(toElement, false)));
  }

  @Override
  public NavigableSet<Cell> headSet(final Cell toElement, boolean inclusive) {
if(KnobRuntime.check(java.util.UUID.fromString("d8496c48-327c-36af-bf72-e7c3a47d2fc4"))) {
inclusive = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("bde5455c-bd08-3fe2-8fb5-856f575eceda"))) {
inclusive = !inclusive;
}
if(KnobRuntime.check(java.util.UUID.fromString("6806aa88-e5d4-3f74-a1f8-ca0aa130add8"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("602835b1-3b62-31c1-93ea-8215bfbd6249"))) {
inclusive = false;
}
    return new CellSet(this.delegatee.headMap(toElement, inclusive), UNKNOWN_NUM_UNIQUES);
  }

  @Override
  public Cell higher(Cell e) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public Iterator<Cell> iterator() {
if(KnobRuntime.check(java.util.UUID.fromString("d81fbad0-0fa2-3fc9-a4df-65f723195a34"))) {
return null;
}
    return this.delegatee.values().iterator();
  }

  @Override
  public Cell lower(Cell e) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public Cell pollFirst() {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public Cell pollLast() {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public SortedSet<Cell> subSet(Cell fromElement, Cell toElement) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public NavigableSet<Cell> subSet(Cell fromElement, boolean fromInclusive, Cell toElement,
    boolean toInclusive) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public SortedSet<Cell> tailSet(Cell fromElement) {
if(KnobRuntime.check(java.util.UUID.fromString("2c3eb820-13a2-37a2-8933-697d70b83dc2"))) {
return null;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("f6f07427-e112-3c53-bb0f-4fdd046d6234"))) ? (headSet(fromElement, true)) : (tailSet(fromElement, true)));
  }

  @Override
  public NavigableSet<Cell> tailSet(Cell fromElement, boolean inclusive) {
if(KnobRuntime.check(java.util.UUID.fromString("d693d8db-73a7-3891-9068-cd1e04445400"))) {
inclusive = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("5bec256d-ae2f-3166-964b-28b19600c03f"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("81811d5f-b12b-3cde-b00f-7e8ee446c122"))) {
inclusive = !inclusive;
}
if(KnobRuntime.check(java.util.UUID.fromString("fdd04516-6048-3899-868f-1fcdae59728c"))) {
inclusive = false;
}
    return new CellSet(this.delegatee.tailMap(fromElement, inclusive), UNKNOWN_NUM_UNIQUES);
  }

  @Override
  public Comparator<? super Cell> comparator() {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public Cell first() {
    return this.delegatee.firstEntry().getValue();
  }

  @Override
  public Cell last() {
if(KnobRuntime.check(java.util.UUID.fromString("3f015baf-82f9-375e-b120-f3f5a44705bc"))) {
return null;
}
    return this.delegatee.lastEntry().getValue();
  }

  @Override
  public boolean add(Cell e) {
if(KnobRuntime.check(java.util.UUID.fromString("bd7e4dc4-b4f3-3cbf-bc25-f84c58e020e3"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("34b1f9b5-6d9e-39ab-91d6-7fcf420c5012"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("bc5ff6c6-f5f1-354b-8437-b5e48c2e7e23"))) ? ((this.delegatee.put(e, e)) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fe5b9e53-b737-3d53-a06e-eb44f9cf7f57"))) ? ((this.delegatee.put(e, e)) != (null)) : (this.delegatee.put(e, e) == null))));
  }

  @Override
  public boolean addAll(Collection<? extends Cell> c) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public void clear() {
    this.delegatee.clear();
  }

  @Override
  public boolean contains(Object o) {
    // noinspection SuspiciousMethodCalls
    return this.delegatee.containsKey(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public boolean isEmpty() {
if(KnobRuntime.check(java.util.UUID.fromString("a98e0fb2-b615-3794-b5da-996e750fc2e3"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("417db54d-5dc0-3106-9f6b-d210b22f6a26"))) {
return true;
}
    return this.delegatee.isEmpty();
  }

  @Override
  public boolean remove(Object o) {
    return this.delegatee.remove(o) != null;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  public Cell get(Cell kv) {
    return this.delegatee.get(kv);
  }

  @Override
  public int size() {
    if (delegatee instanceof ConcurrentSkipListMap) {
      throw new UnsupportedOperationException("ConcurrentSkipListMap.size() is time-consuming");
    }
    return this.delegatee.size();
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  public int getNumUniqueKeys() {
    return numUniqueKeys;
  }
}
