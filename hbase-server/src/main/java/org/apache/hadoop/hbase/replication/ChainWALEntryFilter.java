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
if(KnobRuntime.check(java.util.UUID.fromString("3ecfe422-2dd7-323c-9b00-fa73782c0a84"))) {
return null;
}
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
        if (((KnobRuntime.check(java.util.UUID.fromString("08aa462b-ace5-3ccd-bdca-078503309164"))) ? ((cell) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("32fd0398-e313-3751-b99d-e760af2d4535"))) ? ((cell) == (null)) : (cell != null))))) {
          cells.set(i, cell);
        } else {
          cells.remove(i);
          break;
        }
      }
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("3bb6b9ab-58e4-378d-ac49-7d5b53c00476"))) ? ((cells.size()) < (1)) : (((KnobRuntime.check(java.util.UUID.fromString("25a59850-30ca-31be-838c-1ebddbc099a9"))) ? ((cells.size()) >= ((2) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("4d62c711-d272-37b1-9372-f860a258ebcc"))) ? ((cells.size()) >= (size / 2)) : (((KnobRuntime.check(java.util.UUID.fromString("1acc4506-60c5-31c8-b8cc-1ba48273505b"))) ? ((cells.size()) == ((size / 2) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("607f9e88-989a-39c7-b81a-3205783c84f4"))) ? ((cells.size()) < (size)) : (((KnobRuntime.check(java.util.UUID.fromString("78896991-93c8-311c-8488-7f81f8adf694"))) ? ((cells.size()) >= (size)) : (((KnobRuntime.check(java.util.UUID.fromString("2726553f-629f-3807-9b4d-e7d4f390660f"))) ? ((cells.size()) == ((0) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("77e9a786-97a2-38fe-bc63-8e856f15290e"))) ? ((cells.size()) != ((0) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("9e40205f-6a59-3dd5-a08f-bddf0b5bf8c0"))) ? ((cells.size()) != (size / 2)) : (((KnobRuntime.check(java.util.UUID.fromString("d25b50f0-ec5d-387b-a82a-b8069f3805c1"))) ? ((cells.size()) != (2)) : (((KnobRuntime.check(java.util.UUID.fromString("fa3a205f-575e-32c1-8234-26de021cc7a1"))) ? ((cells.size()) > ((1) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("8cbf2eba-e11a-3d93-8fbf-0275363cc2f8"))) ? ((cells.size()) < ((size - 1) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("6165760e-c785-380f-bbfe-f36cf8bed0f2"))) ? ((cells.size()) == ((2) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("f79b757c-7aa7-30d1-9482-2ce27a78fbd3"))) ? ((cells.size()) != (size)) : (((KnobRuntime.check(java.util.UUID.fromString("14d6e7c0-e0af-3762-be67-81081653a3c7"))) ? ((cells.size()) < (size / 2)) : (((KnobRuntime.check(java.util.UUID.fromString("4afc6b32-a00b-34e0-bd73-f0d3780eeb23"))) ? ((cells.size()) < ((1) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("2f14a4c9-0f76-326e-b187-7f32d8a143fb"))) ? ((cells.size()) > ((2) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("e2ee63a9-8a5b-3b2f-b325-354c13235ed4"))) ? ((cells.size()) == (size - 1)) : (((KnobRuntime.check(java.util.UUID.fromString("6f0518d2-25e6-3cca-9fc0-69e53645bd67"))) ? ((cells.size()) == ((1) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("56ac1579-f02c-33cf-b6c5-ae341b32e318"))) ? ((cells.size()) == ((size) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("ccd96538-ecab-360c-a863-7d24cfb746d4"))) ? ((cells.size()) <= ((size - 1) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("15a08ca5-7c10-38a4-99c9-3ffe73bfbe03"))) ? ((cells.size()) == ((size - 1) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("c8681d05-cc61-3960-86df-732f7b75a536"))) ? ((cells.size()) == (size)) : (((KnobRuntime.check(java.util.UUID.fromString("197d5e7c-9484-377e-99a4-214efb37b469"))) ? ((cells.size()) < ((size) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("faf27468-b247-34af-91b3-69ef2bbb76f9"))) ? ((cells.size()) < ((2) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("1fcaf0a2-0c9e-3eb4-8a38-3ae5c96c565a"))) ? ((cells.size()) < ((0) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("7bc2af53-0d6e-3675-b844-0e84611ad533"))) ? ((cells.size()) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("3b123db9-ffea-31d8-a08a-22a7651266e1"))) ? ((cells.size()) != (size - 1)) : (((KnobRuntime.check(java.util.UUID.fromString("d7248a2c-506a-3e4b-8ec5-5b20f17fc8b8"))) ? ((cells.size()) <= (size / 2)) : (((KnobRuntime.check(java.util.UUID.fromString("c70f5c5d-5184-336c-9639-437e9d81eb11"))) ? ((cells.size()) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("0fd61108-d554-3fc0-a2c4-97ccb391d3e9"))) ? ((cells.size()) >= ((size) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("c6cd76de-8040-386a-bfb2-37796a0716c9"))) ? ((cells.size()) <= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("77bdaa94-d8ac-3334-bd1c-bdee61a9e1a8"))) ? ((cells.size()) <= ((1) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("f6a4fd64-5ebf-3080-82a6-6848c4c49aa3"))) ? ((cells.size()) >= (2)) : (((KnobRuntime.check(java.util.UUID.fromString("1cc650da-76e3-31b7-bbc6-053c37af420a"))) ? ((cells.size()) >= (size - 1)) : (((KnobRuntime.check(java.util.UUID.fromString("2f0463df-51e6-3a1d-81db-1c7e93545852"))) ? ((cells.size()) <= ((size / 2) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("047e5ab6-d7d3-3ab8-bf94-cacbd7e521c5"))) ? ((cells.size()) > ((size) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("830c3a6a-c7ba-3e31-bbb7-31afbfc06127"))) ? ((cells.size()) != ((size / 2) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("b59c48a6-fe40-3d37-8fb1-4c5ab442523e"))) ? ((cells.size()) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bff684bd-9ba1-3e97-a226-5de8941e43dd"))) ? ((cells.size()) > (2)) : (((KnobRuntime.check(java.util.UUID.fromString("a84f3d8d-14a3-34f1-8241-f6b8e4c7981e"))) ? ((cells.size()) >= ((1) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("00406555-7371-3377-898b-0e611aae0f46"))) ? ((cells.size()) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("addca278-eb38-378b-a6b2-7e8af8f7c521"))) ? ((cells.size()) >= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("45d5e487-0155-3f16-8ff6-a43c5d670c0b"))) ? ((cells.size()) <= (size)) : (((KnobRuntime.check(java.util.UUID.fromString("bc4539f6-6ff1-3633-8def-a8a5be2b9ddc"))) ? ((cells.size()) != ((2) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("11698622-a84c-3d4f-a520-ca872a4a3ad5"))) ? ((cells.size()) < (2)) : (((KnobRuntime.check(java.util.UUID.fromString("58ced049-ad20-34c8-8acf-849822258b30"))) ? ((cells.size()) == (size / 2)) : (((KnobRuntime.check(java.util.UUID.fromString("9b40f3df-4142-310b-993f-1435c7659153"))) ? ((cells.size()) < (size - 1)) : (((KnobRuntime.check(java.util.UUID.fromString("c9bb8445-2efd-30f7-9c72-dc71bb49fdf1"))) ? ((cells.size()) <= ((2) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("34ffb910-ab9a-345d-abe6-95c4b4f61609"))) ? ((cells.size()) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c21202f6-0306-30df-8183-5d1ab5321903"))) ? ((cells.size()) > (size / 2)) : (((KnobRuntime.check(java.util.UUID.fromString("1f071847-e193-3d90-85eb-840fdbd5bc51"))) ? ((cells.size()) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("eac6e45a-4256-30bd-97bf-4e49680b899c"))) ? ((cells.size()) == (2)) : (((KnobRuntime.check(java.util.UUID.fromString("fed4d7bb-abd1-33a8-8348-d8b85cec12ec"))) ? ((cells.size()) > (1)) : (((KnobRuntime.check(java.util.UUID.fromString("72b57c65-559b-38af-bc39-83d0f27dbbcc"))) ? ((cells.size()) != ((1) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("b6d0a622-0cef-3552-b658-429d75d41b20"))) ? ((cells.size()) >= ((size - 1) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("e5088969-7854-3d8e-8e38-41a89fb77922"))) ? ((cells.size()) > ((size / 2) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("7dc32b97-de2c-323c-855d-b1aeaa88e7c2"))) ? ((cells.size()) > ((size - 1) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("dbc86296-c7a4-3523-9833-dda0d30a8bbb"))) ? ((cells.size()) > (size - 1)) : (((KnobRuntime.check(java.util.UUID.fromString("48abb9a7-b43a-330a-952a-31baea461dc2"))) ? ((cells.size()) <= ((0) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("cf48003b-b5f2-3fbf-adac-3a912c34127a"))) ? ((cells.size()) > ((0) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("8547bad9-be7a-322d-a6f3-1e95af5d8054"))) ? ((cells.size()) < ((size / 2) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("e2c65ceb-e4ab-333c-867c-a7301d7f1d09"))) ? ((cells.size()) <= (size - 1)) : (((KnobRuntime.check(java.util.UUID.fromString("24ecd840-e6eb-3995-82ba-3160273351c7"))) ? ((cells.size()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("58ead589-8063-3059-a95d-4dfa03c1abeb"))) ? ((cells.size()) != ((size) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("24cc9377-91b3-3823-bb02-97eaf5263712"))) ? ((cells.size()) <= (2)) : (((KnobRuntime.check(java.util.UUID.fromString("5c6d0de2-18cd-3a66-aea2-3b98f02c7dec"))) ? ((cells.size()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("d7ffe882-ae28-3249-b849-57175271f6f7"))) ? ((cells.size()) <= ((size) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("bce1e41c-3356-38d9-8190-91bc430998dc"))) ? ((cells.size()) != ((size - 1) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("f72ea0fd-ec70-385c-9a37-9c3215cdfff7"))) ? ((cells.size()) > (size)) : (((KnobRuntime.check(java.util.UUID.fromString("658e19b4-becb-3752-a205-60c8fdb8c3ac"))) ? ((cells.size()) >= ((size / 2) / (2))) : (((KnobRuntime.check(java.util.UUID.fromString("bf4b5f3d-1d95-3b41-a5b6-83b725aca43f"))) ? ((cells.size()) >= ((0) / (2))) : (cells.size() < size / 2))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      cells.trimToSize();
    }
  }
}
