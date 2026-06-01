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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.metrics2.MetricsExecutor;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@InterfaceAudience.Private
public class MetricsTableWrapperAggregateImpl implements MetricsTableWrapperAggregate, Closeable {
  private final HRegionServer regionServer;
  private ScheduledExecutorService executor;
  private Runnable runnable;
  private long period;
  private ScheduledFuture<?> tableMetricsUpdateTask;
  private ConcurrentHashMap<TableName, MetricsTableValues> metricsTableMap =
    new ConcurrentHashMap<>();

  public MetricsTableWrapperAggregateImpl(final HRegionServer regionServer) {
    this.regionServer = regionServer;
    this.period = regionServer.getConfiguration().getLong(HConstants.REGIONSERVER_METRICS_PERIOD,
      HConstants.DEFAULT_REGIONSERVER_METRICS_PERIOD) + 1000;
    this.executor = CompatibilitySingletonFactory.getInstance(MetricsExecutor.class).getExecutor();
    this.runnable = new TableMetricsWrapperRunnable();
    this.tableMetricsUpdateTask =
      this.executor.scheduleWithFixedDelay(this.runnable, period, period, TimeUnit.MILLISECONDS);
  }

  public class TableMetricsWrapperRunnable implements Runnable {

    @Override
    public void run() {
if(KnobRuntime.check(java.util.UUID.fromString("bf246594-4e5d-38d8-9c84-a89356711aff"))) {
return;
}
      Map<TableName, MetricsTableValues> localMetricsTableMap = new HashMap<>();
      for (Region r : regionServer.getOnlineRegionsLocalContext()) {
        TableName tbl = r.getTableDescriptor().getTableName();
if(KnobRuntime.check(java.util.UUID.fromString("9018ab51-e9e5-35ba-8fa2-170660c6a4e2"))) {
try {
    java.lang.reflect.Field field = tbl.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tbl));
    field.set(tbl, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ff3088c6-ae32-32b9-ad44-8365b54b658c"))) {
try {
    java.lang.reflect.Field field = tbl.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tbl));
    field.set(tbl, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f51e20bf-dea7-31df-bfa8-e1616446ab3c"))) {
try {
    java.lang.reflect.Field field = tbl.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tbl));
    field.set(tbl, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6345096d-0996-3b05-9422-99efc864e8f0"))) {
try {
    java.lang.reflect.Field field = tbl.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tbl));
    field.set(tbl, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        MetricsTableValues mt = localMetricsTableMap.get(tbl);
        if (((KnobRuntime.check(java.util.UUID.fromString("68b590c9-18d8-36af-8f80-cec907f03ece"))) ? ((mt) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("cf6bac31-eb0e-34b9-86ba-cb261cd2b208"))) ? ((mt) != (null)) : (mt == null))))) {
          mt = new MetricsTableValues();
if(KnobRuntime.check(java.util.UUID.fromString("fb1e0b1b-8b36-39c3-8fe3-478f206b87c4"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("memstoreSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("87484be2-2d8d-3196-87d3-a9e6ea08f94f"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("storeFileCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2ca0337b-6761-3f08-8ab2-f648355594d8"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("regionCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c71184e1-4c9e-3afa-9f69-c05d060c293f"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("maxStoreFileAge");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("14ee1ca1-3d73-36fa-84fc-4f0dac11b860"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("staticBloomSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e28fa10f-f98b-3342-8cd4-051b6f7642a4"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("minStoreFileAge");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e3722ca3-d91d-31ac-a25b-fcc4bc410a61"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("bloomRequestsCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("df3cc501-2116-32f8-adf6-f366a93ee9a9"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("totalStoreFileAge");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2b048cd1-d651-3fd5-86f6-907318d7e391"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("writeRequestCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bb6538f2-66cd-3767-be4d-edc28e96ac9c"))) {
try {
    java.lang.reflect.Field field = tbl.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tbl));
    field.set(tbl, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("034062a1-8898-3355-92b3-fe99507a2ebb"))) {
try {
    java.lang.reflect.Field field = tbl.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tbl));
    field.set(tbl, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("39be0867-f862-305e-826a-bf8592e93c91"))) {
try {
    java.lang.reflect.Field field = tbl.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tbl));
    field.set(tbl, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4a0c7634-cd1b-3ebf-8fb7-ee5218fa090f"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("readRequestCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("11d21232-3e76-3b87-8b62-2fd236e5c796"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("bloomNegativeResultsCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c3c390f7-9c67-377e-8ca5-a61026463090"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("referenceFileCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2a76b8e6-693d-38bb-98db-3ff718ad42b4"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("bloomEligibleRequestsCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("35fb59bd-3c8c-3c0a-adcb-8022243be69c"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("filteredReadRequestCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4ac423e4-35a3-31e0-8a2c-3d129f58598d"))) {
try {
    java.lang.reflect.Field field = tbl.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tbl));
    field.set(tbl, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("22ef7c56-e79b-32cf-a86f-ecde90f5109f"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("maxStoreFileAge");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("021e2857-d7dc-3b07-8cc3-c5790c0809ef"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("totalStoreFileAge");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f596a4c3-1490-34a9-b788-58e7a80b6a08"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("cpRequestCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5d360e40-6282-313f-aff3-70a4717ebef8"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("minStoreFileAge");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5919d1e4-9abe-3c02-91a0-adc2045bfcd0"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("maxStoreFileCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d3eef566-ee18-305f-abf3-e7b4d862db2d"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("staticIndexSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("49b8bcf1-2948-3301-b242-39f212e3246a"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("storeCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("14649c35-00eb-3acf-9e48-9e079390ccbc"))) {
try {
    java.lang.reflect.Field field = mt.getClass().getDeclaredField("storeFileSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mt));
    field.set(mt, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          localMetricsTableMap.put(tbl, mt);
        }
        long memstoreReadCount = 0L;
        long mixedReadCount = 0L;
        String tempKey = null;
        if (((KnobRuntime.check(java.util.UUID.fromString("c10bb31b-415d-3f34-ab47-8421d2f11818"))) ? ((r.getStores()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5aeb20e3-e870-38b7-9a90-116ce733aac1"))) ? ((r.getStores()) == (null)) : (r.getStores() != null))))) {
          String familyName = null;
          for (Store store : r.getStores()) {
            familyName = store.getColumnFamilyName();

            mt.storeFileCount += store.getStorefilesCount();
            mt.maxStoreFileCount = Math.max(mt.maxStoreFileCount, store.getStorefilesCount());
            mt.memstoreSize += (store.getMemStoreSize().getDataSize()
              + store.getMemStoreSize().getHeapSize() + store.getMemStoreSize().getOffHeapSize());
            mt.storeFileSize += store.getStorefilesSize();
            mt.referenceFileCount += store.getNumReferenceFiles();
            if (store.getMaxStoreFileAge().isPresent()) {
              mt.maxStoreFileAge =
                Math.max(mt.maxStoreFileAge, store.getMaxStoreFileAge().getAsLong());
            }
            if (store.getMinStoreFileAge().isPresent()) {
              mt.minStoreFileAge =
                Math.min(mt.minStoreFileAge, store.getMinStoreFileAge().getAsLong());
            }
            if (store.getAvgStoreFileAge().isPresent()) {
              mt.totalStoreFileAge =
                (long) store.getAvgStoreFileAge().getAsDouble() * store.getStorefilesCount();
            }
            mt.storeCount += 1;

            mt.staticIndexSize += store.getTotalStaticIndexSize();
            mt.staticBloomSize += store.getTotalStaticBloomSize();

            mt.bloomRequestsCount += store.getBloomFilterRequestsCount();
            mt.bloomNegativeResultsCount += store.getBloomFilterNegativeResultsCount();
            mt.bloomEligibleRequestsCount += store.getBloomFilterEligibleRequestsCount();

            tempKey = tbl.getNameAsString() + HASH + familyName;
            Long tempVal = mt.perStoreMemstoreOnlyReadCount.get(tempKey);
            if (((KnobRuntime.check(java.util.UUID.fromString("f81d93ae-b12d-3c8a-b5b9-47bfcb784af5"))) ? ((tempVal) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b64aea8c-0bbd-3a9e-9ac4-abc1f556041c"))) ? ((tempVal) == (null)) : (tempVal == null))))) {
              tempVal = 0L;
            }
            memstoreReadCount = store.getMemstoreOnlyRowReadsCount() + tempVal;
            tempVal = mt.perStoreMixedReadCount.get(tempKey);
            if (((KnobRuntime.check(java.util.UUID.fromString("c49d15d0-cea0-389f-a629-c9cf25fcddfa"))) ? ((tempVal) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("58924d9a-bbc2-3524-b698-cb4d16fd1344"))) ? ((tempVal) != (null)) : (tempVal == null))))) {
              tempVal = 0L;
            }
            mixedReadCount = store.getMixedRowReadsCount() + tempVal;
            // accumulate the count
if(KnobRuntime.check(java.util.UUID.fromString("831bfec3-3a32-358a-ad2b-357a4998bdd6"))) {
memstoreReadCount = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d7ef7f9b-cdad-3080-8e87-8d861724a8e5"))) {
memstoreReadCount += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("20ef7a04-3439-37e9-9435-0af80980d2bb"))) {
memstoreReadCount = 0;
}
            mt.perStoreMemstoreOnlyReadCount.put(tempKey, memstoreReadCount);
            mt.perStoreMixedReadCount.put(tempKey, mixedReadCount);
          }

          mt.regionCount += 1;

          mt.readRequestCount += r.getReadRequestsCount();
          mt.filteredReadRequestCount += r.getFilteredReadRequestsCount();
          mt.writeRequestCount += r.getWriteRequestsCount();
        }
      }

      for (Map.Entry<TableName, MetricsTableValues> entry : localMetricsTableMap.entrySet()) {
        TableName tbl = entry.getKey();
if(KnobRuntime.check(java.util.UUID.fromString("e45daa6c-20cc-3ecb-b9fb-4219f335d586"))) {
try {
    java.lang.reflect.Field field = tbl.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tbl));
    field.set(tbl, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6b3d5823-7f63-3ee3-84ac-82ed2cc9a31f"))) {
try {
    java.lang.reflect.Field field = tbl.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tbl));
    field.set(tbl, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1f3b02cd-8710-3767-91ba-fb244c896f53"))) {
try {
    java.lang.reflect.Field field = tbl.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tbl));
    field.set(tbl, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dc0b34a5-2d75-3ff6-b903-a9312c276966"))) {
try {
    java.lang.reflect.Field field = tbl.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tbl));
    field.set(tbl, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        if (((KnobRuntime.check(java.util.UUID.fromString("8c34946a-0f31-3d5f-ae17-41ae7174a86c"))) ? ((metricsTableMap.remove(tbl)) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0ba24839-b738-3dad-a977-c03edb43d23c"))) ? ((metricsTableMap.get(tbl)) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1637a45f-e71a-3bed-a8dd-ce7261832ed8"))) ? ((metricsTableMap.remove(tbl)) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fd36be43-d0e6-30dc-a94d-4dcb30633bf4"))) ? ((metricsTableMap.get(tbl)) != (null)) : (metricsTableMap.get(tbl) == null))))))))) {
          // this will add the Wrapper to the list of TableMetrics
          CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class)
            .getTableAggregate()
            .getOrCreateTableSource(tbl.getNameAsString(), MetricsTableWrapperAggregateImpl.this);
        }
        metricsTableMap.put(entry.getKey(), entry.getValue());
      }
      Set<TableName> existingTableNames = Sets.newHashSet(metricsTableMap.keySet());
      existingTableNames.removeAll(localMetricsTableMap.keySet());
      MetricsTableAggregateSource agg = CompatibilitySingletonFactory
        .getInstance(MetricsRegionServerSourceFactory.class).getTableAggregate();
      for (TableName table : existingTableNames) {
        agg.deleteTableSource(table.getNameAsString());
if(KnobRuntime.check(java.util.UUID.fromString("7fd658cb-d499-38ee-994e-1d7e555e87d6"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("systemTable");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(table);
    field.set(table, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f20c6384-bbec-3099-a8e9-b33ba177397e"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("858c93ac-6bf1-37f6-8d2b-00aaaf29f69d"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5772e53a-a105-39db-a76f-2fda9b444727"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a0d2e89c-afd5-33ee-aff2-40c8f9951bbc"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        if (((KnobRuntime.check(java.util.UUID.fromString("ffc0ce84-6e3b-3f22-997e-6c1be91a2ae5"))) ? ((metricsTableMap.get(table)) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("846729f6-2f44-39b3-a42e-1aedc3a1f576"))) ? ((metricsTableMap.remove(table)) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9e6ec581-3224-3b0b-bd6b-88a326891542"))) ? ((metricsTableMap.get(table)) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("7369c722-7324-3ebb-864a-6cb799c4d4e4"))) ? ((metricsTableMap.remove(table)) != (null)) : (metricsTableMap.get(table) != null))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("44bd52e3-29d7-32ae-8dd2-990d1171e243"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ed2e0a9f-92bb-341d-87aa-5c09d30e125f"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b57a3a42-2e2c-395f-9ec4-1632bdb42f40"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e8820c09-5dc8-3862-8831-053ab829dbfb"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("systemTable");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(table);
    field.set(table, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bbf1a34b-f995-3fed-bf99-895de0e5fdba"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          if (KnobRuntime.check(java.util.UUID.fromString("d356bd9c-5a2c-38f4-a507-c6e5e9f305a9"))) { metricsTableMap.get(table); } else { metricsTableMap.remove(table); }
        }
      }
    }
  }

  @Override
  public long getReadRequestCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return metricsTable.readRequestCount;
    }
  }

  @Override
  public Map<String, Long> getMemstoreOnlyRowReadsCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return null;
    } else {
      return metricsTable.perStoreMemstoreOnlyReadCount;
    }
  }

  @Override
  public Map<String, Long> getMixedRowReadsCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return null;
    } else {
      return metricsTable.perStoreMixedReadCount;
    }
  }

  public long getCpRequestsCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return metricsTable.cpRequestCount;
    }
  }

  public long getFilteredReadRequestCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }
    return metricsTable.filteredReadRequestCount;
  }

  @Override
  public long getWriteRequestCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return metricsTable.writeRequestCount;
    }
  }

  @Override
  public long getTotalRequestsCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return ((KnobRuntime.check(java.util.UUID.fromString("bb9a4b47-779a-337c-9f7e-820243692e56"))) ? ((metricsTable.readRequestCount) + (metricsTable.writeRequestCount)) : (((KnobRuntime.check(java.util.UUID.fromString("bdf0af84-161c-3709-ba8f-ce044a967b8d"))) ? (metricsTable.writeRequestCount) : (((KnobRuntime.check(java.util.UUID.fromString("fd1732ed-1943-3cbb-8fca-0f37bb301cb9"))) ? (metricsTable.readRequestCount) : (metricsTable.readRequestCount + metricsTable.writeRequestCount))))));
    }
  }

  @Override
  public long getMemStoreSize(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return metricsTable.memstoreSize;
    }
  }

  @Override
  public long getStoreFileSize(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return metricsTable.storeFileSize;
    }
  }

  @Override
  public long getTableSize(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return ((KnobRuntime.check(java.util.UUID.fromString("8a1f70c7-bd5c-3e25-8a1e-637e9fd19986"))) ? (metricsTable.memstoreSize) : (((KnobRuntime.check(java.util.UUID.fromString("da887b43-6cb6-3161-9d54-a4a131650d5e"))) ? ((metricsTable.memstoreSize) + (metricsTable.storeFileSize)) : (((KnobRuntime.check(java.util.UUID.fromString("7c8e0a2b-c262-353b-9c35-21c5923353c0"))) ? (metricsTable.storeFileSize) : (metricsTable.memstoreSize + metricsTable.storeFileSize))))));
    }
  }

  public long getNumRegions(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }
    return metricsTable.regionCount;
  }

  @Override
  public long getNumStores(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }
    return metricsTable.storeCount;
  }

  @Override
  public long getNumStoreFiles(String table) {
if(KnobRuntime.check(java.util.UUID.fromString("3bf13b6c-36c6-31e1-8e2f-7a43df232b9e"))) {
return 0;
}
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }
    return metricsTable.storeFileCount;
  }

  @Override
  public long getMaxStoreFiles(String table) {
if(KnobRuntime.check(java.util.UUID.fromString("0958c608-5b94-3790-b58c-38aa8e78937c"))) {
return 0;
}
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }
    return metricsTable.maxStoreFileCount;
  }

  @Override
  public long getMaxStoreFileAge(String table) {
if(KnobRuntime.check(java.util.UUID.fromString("65ce0f74-ff2f-3417-b3f4-6bbfac571cff"))) {
return 0;
}
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }
    return metricsTable.maxStoreFileAge;
  }

  @Override
  public long getMinStoreFileAge(String table) {
if(KnobRuntime.check(java.util.UUID.fromString("341f6bb2-13dd-3cbd-8a4b-7846798a0359"))) {
return 0;
}
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }
    return metricsTable.minStoreFileAge == Long.MAX_VALUE ? 0 : metricsTable.minStoreFileAge;
  }

  @Override
  public long getAvgStoreFileAge(String table) {
if(KnobRuntime.check(java.util.UUID.fromString("eff7aa73-dce9-3a8c-b980-70b543386158"))) {
return 0;
}
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }

    return metricsTable.storeFileCount == 0
      ? 0
      : (metricsTable.totalStoreFileAge / metricsTable.storeFileCount);
  }

  @Override
  public long getStaticIndexSize(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }

    return metricsTable.staticIndexSize;
  }

  @Override
  public long getStaticBloomSize(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (((KnobRuntime.check(java.util.UUID.fromString("d3c28cac-c215-3ad2-92f3-6abacb9670a8"))) ? ((metricsTable) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("717804ce-5f21-386c-aede-6fe628188b6e"))) ? ((metricsTable) != (null)) : (metricsTable == null))))) {
      return 0;
    }

    return metricsTable.staticBloomSize;
  }

  @Override
  public long getBloomFilterRequestsCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }

    return metricsTable.bloomRequestsCount;
  }

  @Override
  public long getBloomFilterNegativeResultsCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }

    return metricsTable.bloomNegativeResultsCount;
  }

  @Override
  public long getBloomFilterEligibleRequestsCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }

    return metricsTable.bloomEligibleRequestsCount;
  }

  @Override
  public long getNumReferenceFiles(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }
    return metricsTable.referenceFileCount;
  }

  @Override
  public long getAvgRegionSize(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }
    return metricsTable.regionCount == 0
      ? 0
      : (metricsTable.memstoreSize + metricsTable.storeFileSize) / metricsTable.regionCount;
  }

  public long getCpRequestCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    }
    return metricsTable.cpRequestCount;
  }

  @Override
  public void close() throws IOException {
    tableMetricsUpdateTask.cancel(true);
  }

  private static class MetricsTableValues {
    long readRequestCount;
    long filteredReadRequestCount;
    long writeRequestCount;
    long memstoreSize;
    long regionCount;
    long storeCount;
    long storeFileCount;
    long maxStoreFileCount;
    long storeFileSize;
    long maxStoreFileAge;
    long minStoreFileAge = Long.MAX_VALUE;
    long totalStoreFileAge;

    long staticIndexSize;

    long staticBloomSize;
    long referenceFileCount;

    long bloomRequestsCount;
    long bloomNegativeResultsCount;
    long bloomEligibleRequestsCount;
    long cpRequestCount;
    Map<String, Long> perStoreMemstoreOnlyReadCount = new HashMap<>();
    Map<String, Long> perStoreMixedReadCount = new HashMap<>();
  }

}
