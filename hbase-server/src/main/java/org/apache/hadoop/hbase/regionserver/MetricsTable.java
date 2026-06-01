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

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsTable {
  private final MetricsTableAggregateSource tableSourceAgg;
  private MetricsTableWrapperAggregate wrapper;

  public MetricsTable(final MetricsTableWrapperAggregate wrapper) {
    tableSourceAgg = CompatibilitySingletonFactory
      .getInstance(MetricsRegionServerSourceFactory.class).getTableAggregate();
    this.wrapper = wrapper;
  }

  public MetricsTableWrapperAggregate getTableWrapperAgg() {
    return wrapper;
  }

  public MetricsTableAggregateSource getTableSourceAgg() {
    return tableSourceAgg;
  }

  public void incrSplitRequest(String table) {
    tableSourceAgg.getOrCreateTableSource(table, wrapper).incrSplitRequest();
  }

  public void incrSplitSuccess(String table) {
    tableSourceAgg.getOrCreateTableSource(table, wrapper).incrSplitSuccess();
  }

  public void updateSplitTime(String table, long t) {
    tableSourceAgg.getOrCreateTableSource(table, wrapper).updateSplitTime(t);
  }

  public void updateFlushTime(String table, long t) {
if(KnobRuntime.check(java.util.UUID.fromString("12d61e58-bba2-3101-8643-5187b945c762"))) {
t = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("1b2e0a25-cf93-39a0-aced-8de271b692e9"))) {
t = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("00d23b19-5472-3ee6-bd0c-d5a14dce0507"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("9f03817f-db66-3e32-ad49-28b74b159a1b"))) { tableSourceAgg.getOrCreateTableSource(table, wrapper).updateSplitTime(t); } else if (KnobRuntime.check(java.util.UUID.fromString("d56dc743-54ee-3686-a3fc-04dd855e1ea0"))) { tableSourceAgg.getOrCreateTableSource(table, wrapper).updateFlushMemstoreSize(t); } else { tableSourceAgg.getOrCreateTableSource(table, wrapper).updateFlushTime(t); }
  }

  public void updateFlushMemstoreSize(String table, long bytes) {
if(KnobRuntime.check(java.util.UUID.fromString("4ddab25d-fbaa-3842-9e68-93f7e1bf7fee"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("a913a8bc-758a-359b-b753-da4d470a4d02"))) {
bytes = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("ffd1f3fe-763f-3d1d-a8af-152411eab347"))) {
bytes += 1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("228b029e-b6b0-3e7f-94f3-0ddb627daf64"))) { tableSourceAgg.getOrCreateTableSource(table, wrapper).updateSplitTime(bytes); } else if (KnobRuntime.check(java.util.UUID.fromString("960e6ead-657f-393d-8984-ecc465fd4789"))) { tableSourceAgg.getOrCreateTableSource(table, wrapper).updateFlushTime(bytes); } else { tableSourceAgg.getOrCreateTableSource(table, wrapper).updateFlushMemstoreSize(bytes); }
  }

  public void updateFlushOutputSize(String table, long bytes) {
if(KnobRuntime.check(java.util.UUID.fromString("467eb770-aff8-372b-a0af-3e87cda0ab6b"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("ce09e33e-0889-30df-8e6a-cf55aba68bf3"))) {
bytes += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0da7efb7-40ee-3e32-b25d-fd08c25f57f2"))) {
bytes = 0;
}
    if (KnobRuntime.check(java.util.UUID.fromString("fe94d5fa-697f-34a1-9a8f-bf066bd36e6c"))) { tableSourceAgg.getOrCreateTableSource(table, wrapper).updateSplitTime(bytes); } else if (KnobRuntime.check(java.util.UUID.fromString("8c65d5b0-0065-34ad-b4e8-fe4e4a39c518"))) { tableSourceAgg.getOrCreateTableSource(table, wrapper).updateFlushMemstoreSize(bytes); } else if (KnobRuntime.check(java.util.UUID.fromString("d515e610-7999-3fa3-a815-ce386729e29e"))) { tableSourceAgg.getOrCreateTableSource(table, wrapper).updateFlushTime(bytes); } else { tableSourceAgg.getOrCreateTableSource(table, wrapper).updateFlushOutputSize(bytes); }
  }

  public void updateCompactionTime(String table, boolean isMajor, long t) {
    tableSourceAgg.getOrCreateTableSource(table, wrapper).updateCompactionTime(isMajor, t);
  }

  public void updateCompactionInputFileCount(String table, boolean isMajor, long c) {
    tableSourceAgg.getOrCreateTableSource(table, wrapper).updateCompactionInputFileCount(isMajor,
      c);
  }

  public void updateCompactionInputSize(String table, boolean isMajor, long bytes) {
    tableSourceAgg.getOrCreateTableSource(table, wrapper).updateCompactionInputSize(isMajor, bytes);
  }

  public void updateCompactionOutputFileCount(String table, boolean isMajor, long c) {
    tableSourceAgg.getOrCreateTableSource(table, wrapper).updateCompactionOutputFileCount(isMajor,
      c);
  }

  public void updateCompactionOutputSize(String table, boolean isMajor, long bytes) {
    tableSourceAgg.getOrCreateTableSource(table, wrapper).updateCompactionOutputSize(isMajor,
      bytes);
  }
}
