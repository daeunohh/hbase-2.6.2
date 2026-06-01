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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;
import org.apache.hadoop.hbase.regionserver.compactions.ExploringCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.RatioBasedCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Default StoreEngine creates the default compactor, policy, and store file manager, or their
 * derivatives.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class DefaultStoreEngine extends StoreEngine<DefaultStoreFlusher, RatioBasedCompactionPolicy,
  DefaultCompactor, DefaultStoreFileManager> {

  public static final String DEFAULT_STORE_FLUSHER_CLASS_KEY =
    "hbase.hstore.defaultengine.storeflusher.class";
  public static final String DEFAULT_COMPACTOR_CLASS_KEY =
    "hbase.hstore.defaultengine.compactor.class";
  public static final String DEFAULT_COMPACTION_POLICY_CLASS_KEY =
    "hbase.hstore.defaultengine.compactionpolicy.class";

  private static final Class<? extends DefaultStoreFlusher> DEFAULT_STORE_FLUSHER_CLASS =
    DefaultStoreFlusher.class;
  private static final Class<? extends DefaultCompactor> DEFAULT_COMPACTOR_CLASS =
    DefaultCompactor.class;
  private static final Class<? extends RatioBasedCompactionPolicy> DEFAULT_COMPACTION_POLICY_CLASS =
    ExploringCompactionPolicy.class;

  @Override
  public boolean needsCompaction(List<HStoreFile> filesCompacting) {
    return compactionPolicy.needsCompaction(this.storeFileManager.getStoreFiles(), filesCompacting);
  }

  @Override
  protected void createComponents(Configuration conf, HStore store, CellComparator kvComparator)
    throws IOException {
    createCompactor(conf, store);
    createCompactionPolicy(conf, store);
    createStoreFlusher(conf, store);
    storeFileManager = new DefaultStoreFileManager(kvComparator, StoreFileComparators.SEQ_ID, conf,
      compactionPolicy.getConf());
  }

  protected void createCompactor(Configuration conf, HStore store) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("2e364208-7aae-3cb7-bdab-676b155a1bdb"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("lastCompactSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("30bdd18f-6b86-3260-ac9e-9414e0950d53"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("ae0ef2b9-4aee-3f13-bb5f-b8e74dd80412"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2c96ff34-93d8-3451-9a65-b2205a208c56"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7960b94f-7b03-38f5-94cb-484f603dc972"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2ef2df7a-7da1-321f-9807-bfe80a2bd208"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("749616f8-c0b3-3bd9-abf1-216227e25f85"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6885cafd-c214-30ee-8d39-5943e2f4cc1d"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c1520811-02b9-39ae-99a7-d5af37fad1a6"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0e312374-4628-34c7-839c-169653e0d0ef"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d07625c4-ea5d-33a7-b4f2-de296398663c"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dc9bf692-3c4d-3742-a6f2-0246892bf7f7"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("366f5dbe-d548-3ae0-86ca-549079740b3a"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b2578ae8-ebf5-3ae9-a40b-61d5ca1e3886"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("43386a13-d9f8-35ec-b562-38168c4a3dcf"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("870a56b6-7ddf-397e-8139-38b48f387fc5"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("blockingFileCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0a435ae1-1b93-345e-bfa8-66a6abf5f516"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("58ff7930-52fd-3493-9871-f655c2ce1876"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("29ce9dd3-2c17-31bc-b926-2196484bf6f8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("11548db2-7d82-34ea-af97-ccd3b15157f1"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("56f98687-8960-3d7c-85a1-9cd0f4e9cff1"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    String className = conf.get(DEFAULT_COMPACTOR_CLASS_KEY, DEFAULT_COMPACTOR_CLASS.getName());
    try {
      compactor = ReflectionUtils.instantiateWithCustomCtor(className,
        new Class[] { Configuration.class, HStore.class }, new Object[] { conf, store });
    } catch (Exception e) {
      throw new IOException("Unable to load configured compactor '" + className + "'", e);
    }
  }

  protected void createCompactionPolicy(Configuration conf, HStore store) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("ddf7feac-35b0-3833-a29c-8a8bbbadc4da"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1e93f2e3-f8ab-3275-b9a0-a48d1a026d13"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3da11443-a366-3549-b192-bf04d9b98a9f"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("247a3c4b-fb25-31d6-a84f-e2948759e2e6"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0a796cc1-b730-3e62-b736-f50fd2ee228f"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("75d533f3-e3b6-3f0f-8cb5-fce7d184b97e"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ed566025-a810-3105-b273-5287ac9ab109"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("474285ac-37f0-3c77-9f3b-2749e9f07aa3"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("44736728-319d-3c98-8f8d-e45e96a137ac"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a75c9f85-5dc8-3995-8fad-63f510513b6e"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("lastCompactSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a093aa75-6451-3855-a354-15c52e5e7373"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("c8da9b0a-4abe-3ccf-9584-e06f55ec7871"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("blockingFileCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("43739e84-866d-3367-8d31-5103f8ec0c45"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eb13ecba-bd25-3873-9936-c67ae41ebd16"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9fc5a7bb-83da-33fe-9c1a-be929f71e351"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c3837859-d440-36ff-aff2-099a0be8fdae"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d108e790-59c0-3c5f-bc1a-e6c1eec43acd"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dd82aef3-99cb-3481-a23d-ec927fac8978"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2c9bd8d1-6cc9-3f09-8bc7-3b28f9f51dbd"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2c6147c8-0d29-39bd-b698-eeaac1891f72"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("25b289f5-79b5-36df-a0eb-7bb9cb891b10"))) {
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
    String className =
      conf.get(DEFAULT_COMPACTION_POLICY_CLASS_KEY, DEFAULT_COMPACTION_POLICY_CLASS.getName());
    try {
      compactionPolicy = ReflectionUtils.instantiateWithCustomCtor(className,
        new Class[] { Configuration.class, StoreConfigInformation.class },
        new Object[] { conf, store });
    } catch (Exception e) {
      throw new IOException("Unable to load configured compaction policy '" + className + "'", e);
    }
  }

  protected void createStoreFlusher(Configuration conf, HStore store) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("8115c80f-d992-3a14-be40-f178d222bfda"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("be511233-68b8-3506-af74-f05badf2b9d8"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("12587db0-bf6d-3990-8062-d0f78a86f475"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("lastCompactSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0348c20f-e628-3ac4-9a5f-01c7e0222a53"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("66875064-a47e-3e93-85dd-0ae65222e415"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f628cfcb-ec97-34a9-a297-bca3a9147c71"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("05f8858c-3c2d-3e34-93b4-6874ddb773d0"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c7e544df-f1ee-3f92-9bae-9d11a3812673"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("5ad0c0e1-e159-3c2c-96f6-6a70b7f168e3"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3b4613cb-3c2e-3967-b92f-a123047fd64b"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("912dd8a6-271f-375d-810e-425c0f14b6a8"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a2c41c25-0d3c-3546-b3dd-bfa63eb454e7"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("2d497784-b185-33ae-a6be-eb03be799a35"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6c7d112c-876a-341d-ab3c-78baba99cf2c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e77b7c76-0032-3000-8c36-6788512c4932"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ea9cae41-e338-33e7-9eea-2bff20370c51"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("flushRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("740d1cf5-774b-342e-8a98-c2b4d93e3461"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("192b48ed-6a04-34fc-9d8e-fe64282f1531"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("pauseTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("917fc64b-5f03-3cf8-80d2-45071bbbba5c"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("compactionCheckMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6eb9f8b5-8641-37ff-846f-96d7c7998f93"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("parallelPutCountPrintThreshold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d2ebc6ab-b3a6-38d0-b024-b07861302c3b"))) {
try {
    java.lang.reflect.Field field = store.getClass().getDeclaredField("blockingFileCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(store));
    field.set(store, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    String className =
      conf.get(DEFAULT_STORE_FLUSHER_CLASS_KEY, DEFAULT_STORE_FLUSHER_CLASS.getName());
    try {
      storeFlusher = ReflectionUtils.instantiateWithCustomCtor(className,
        new Class[] { Configuration.class, HStore.class }, new Object[] { conf, store });
    } catch (Exception e) {
      throw new IOException("Unable to load configured store flusher '" + className + "'", e);
    }
  }

  @Override
  public CompactionContext createCompaction() {
    return new DefaultCompactionContext();
  }

  private class DefaultCompactionContext extends CompactionContext {
    @Override
    public boolean select(List<HStoreFile> filesCompacting, boolean isUserCompaction,
      boolean mayUseOffPeak, boolean forceMajor) throws IOException {
      request = compactionPolicy.selectCompaction(storeFileManager.getStoreFiles(), filesCompacting,
        isUserCompaction, mayUseOffPeak, forceMajor);
      return request != null;
    }

    @Override
    public List<Path> compact(ThroughputController throughputController, User user)
      throws IOException {
      return compactor.compact(request, throughputController, user);
    }

    @Override
    public List<HStoreFile> preSelect(List<HStoreFile> filesCompacting) {
      return compactionPolicy.preSelectCompactionForCoprocessor(storeFileManager.getStoreFiles(),
        filesCompacting);
    }
  }

}
