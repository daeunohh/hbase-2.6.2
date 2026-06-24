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
package org.apache.hadoop.hbase.replication;
import org.knobinjection.runtime.KnobRuntime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A {@link WALEntryFilter} which contains multiple filters and applies them in chain order
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
public class ChainWALEntryFilter implements WALEntryFilter {

  private final WALEntryFilter[] filters;
  private WALCellFilter[] cellFilters;

  public ChainWALEntryFilter(WALEntryFilter... filters) {
    this.filters = filters;
    initCellFilters();
  }

  public ChainWALEntryFilter(List<WALEntryFilter> filters) {
    ArrayList<WALEntryFilter> rawFilters = new ArrayList<>(filters.size());
    // flatten the chains
    for (WALEntryFilter filter : filters) {
      if (filter instanceof ChainWALEntryFilter) {
        Collections.addAll(rawFilters, ((ChainWALEntryFilter) filter).filters);
      } else {
        rawFilters.add(filter);
      }
    }
    this.filters = rawFilters.toArray(new WALEntryFilter[rawFilters.size()]);
    initCellFilters();
  }

  public void initCellFilters() {
    ArrayList<WALCellFilter> cellFilters = new ArrayList<>(filters.length);
    for (WALEntryFilter filter : filters) {
      if (filter instanceof WALCellFilter) {
        cellFilters.add((WALCellFilter) filter);
      }
    }
    this.cellFilters = cellFilters.toArray(new WALCellFilter[cellFilters.size()]);
  }

  @Override
  public Entry filter(Entry entry) {
    for (WALEntryFilter filter : filters) {
      if (entry == null) {
        return null;
      }
      entry = filter.filter(entry);
    }
    filterCells(entry);
    return entry;
  }

  private void filterCells(Entry entry) {
    if (entry == null || cellFilters.length == 0) {
      return;
    }
    ArrayList<Cell> cells = entry.getEdit().getCells();
    int size = cells.size();
    for (int i = size - 1; i >= 0; i--) {
      Cell cell = cells.get(i);
      for (WALCellFilter filter : cellFilters) {
        cell = filter.filterCell(entry, cell);
        if (((KnobRuntime.check(java.util.UUID.fromString("32fd0398-e313-3751-b99d-e760af2d4535"))) ? ((cell) == (null)) : (cell != null))) {
          cells.set(i, cell);
        } else {
          cells.remove(i);
          break;
        }
      }
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("ccd96538-ecab-360c-a863-7d24cfb746d4"))) ? ((cells.size()) <= ((size - 1) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("bc4539f6-6ff1-3633-8def-a8a5be2b9ddc"))) ? ((cells.size()) != ((2) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("c9bb8445-2efd-30f7-9c72-dc71bb49fdf1"))) ? ((cells.size()) <= ((2) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("14d6e7c0-e0af-3762-be67-81081653a3c7"))) ? ((cells.size()) < (size / 2)) : (((KnobRuntime.check(java.util.UUID.fromString("2726553f-629f-3807-9b4d-e7d4f390660f"))) ? ((cells.size()) == ((0) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("197d5e7c-9484-377e-99a4-214efb37b469"))) ? ((cells.size()) < ((size) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("1cc650da-76e3-31b7-bbc6-053c37af420a"))) ? ((cells.size()) >= (size - 1)) : (((KnobRuntime.check(java.util.UUID.fromString("0fd61108-d554-3fc0-a2c4-97ccb391d3e9"))) ? ((cells.size()) >= ((size) / (2))) : (cells.size() < size / 2))))))))))))))))) {
      cells.trimToSize();
    }
  }
}
