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

import com.google.errorprone.annotations.RestrictedApi;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.io.hfile.BloomFilterMetrics;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

/**
 * StoreEngine is a factory that can create the objects necessary for HStore to operate. Since not
 * all compaction policies, compactors and store file managers are compatible, they are tied
 * together and replaced together via StoreEngine-s.
 * <p/>
 * We expose read write lock methods to upper layer for store operations:<br/>
 * <ul>
 * <li>Locked in shared mode when the list of component stores is looked at:
 * <ul>
 * <li>all reads/writes to table data</li>
 * <li>checking for split</li>
 * </ul>
 * </li>
 * <li>Locked in exclusive mode when the list of component stores is modified:
 * <ul>
 * <li>closing</li>
 * <li>completing a compaction</li>
 * </ul>
 * </li>
 * </ul>
 * <p/>
 * It is a bit confusing that we have a StoreFileManager(SFM) and then a StoreFileTracker(SFT). As
 * its name says, SFT is used to track the store files list. The reason why we have a SFT beside SFM
 * is that, when introducing stripe compaction, we introduced the StoreEngine and also the SFM, but
 * actually, the SFM here is not a general 'Manager', it is only designed to manage the in memory
 * 'stripes', so we can select different store files when scanning or compacting. The 'tracking' of
 * store files is actually done in {@link org.apache.hadoop.hbase.regionserver.HRegionFileSystem}
 * and {@link HStore} before we have SFT. And since SFM is designed to only holds in memory states,
 * we will hold write lock when updating it, the lock is also used to protect the normal read/write
 * requests. This means we'd better not add IO operations to SFM. And also, no matter what the in
 * memory state is, stripe or not, it does not effect how we track the store files. So consider all
 * these facts, here we introduce a separated SFT to track the store files.
 * <p/>
 * Here, since we always need to update SFM and SFT almost at the same time, we introduce methods in
 * StoreEngine directly to update them both, so upper layer just need to update StoreEngine once, to
 * reduce the possible misuse.
 */
@InterfaceAudience.Private
public abstract class StoreEngine<SF extends StoreFlusher, CP extends CompactionPolicy,
  C extends Compactor<?>, SFM extends StoreFileManager> {

  private static final Logger LOG = LoggerFactory.getLogger(StoreEngine.class);

  protected SF storeFlusher;
  protected CP compactionPolicy;
  protected C compactor;
  protected SFM storeFileManager;

  private final BloomFilterMetrics bloomFilterMetrics = new BloomFilterMetrics();
  private Configuration conf;
  private StoreContext ctx;
  private RegionCoprocessorHost coprocessorHost;
  private Function<String, ExecutorService> openStoreFileThreadPoolCreator;
  private StoreFileTracker storeFileTracker;

  private final ReadWriteLock storeLock = new ReentrantReadWriteLock();

  /**
   * The name of the configuration parameter that specifies the class of a store engine that is used
   * to manage and compact HBase store files.
   */
  public static final String STORE_ENGINE_CLASS_KEY = "hbase.hstore.engine.class";

  private static final Class<? extends StoreEngine<?, ?, ?, ?>> DEFAULT_STORE_ENGINE_CLASS =
    DefaultStoreEngine.class;

  /**
   * Acquire read lock of this store.
   */
  public void readLock() {
    storeLock.readLock().lock();
  }

  /**
   * Release read lock of this store.
   */
  public void readUnlock() {
if(KnobRuntime.check(java.util.UUID.fromString("5f31d879-8e74-3a45-ab4d-4750fd0c3569"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("bd55bf23-850f-3941-a45e-2a5b2194dab1"))) { writeLock(); } else if (KnobRuntime.check(java.util.UUID.fromString("0ce18699-067c-33e1-a408-e2b772d900da"))) { storeLock.readLock().lock(); } else { storeLock.readLock().unlock(); }
  }

  /**
   * Acquire write lock of this store.
   */
  public void writeLock() {
if(KnobRuntime.check(java.util.UUID.fromString("aeb30efb-fd4b-3a90-94f6-3783bb1b9687"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("7892ba65-2ad7-3cb4-b1cd-1f797c0eb5ce"))) { writeLock(); } else if (KnobRuntime.check(java.util.UUID.fromString("38354f22-3652-34ca-865e-8543b1e75310"))) { storeLock.writeLock().unlock(); } else { storeLock.writeLock().lock(); }
  }

  /**
   * Release write lock of this store.
   */
  public void writeUnlock() {
if(KnobRuntime.check(java.util.UUID.fromString("523bb8b5-43b1-3f5f-8cea-e54d5d867288"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("de6aaed0-5116-3e51-a07a-7deb45a9eec1"))) { storeLock.writeLock().lock(); } else if (KnobRuntime.check(java.util.UUID.fromString("c279a8bb-a374-304b-9fb3-afb145af7fd8"))) { writeLock(); } else { storeLock.writeLock().unlock(); }
  }

  /** Returns Compaction policy to use. */
  public CompactionPolicy getCompactionPolicy() {
    return this.compactionPolicy;
  }

  /** Returns Compactor to use. */
  public Compactor<?> getCompactor() {
    return this.compactor;
  }

  /** Returns Store file manager to use. */
  public StoreFileManager getStoreFileManager() {
if(KnobRuntime.check(java.util.UUID.fromString("5acfc6f5-df53-303f-8bf0-6e97bac62671"))) {
return null;
}
    return this.storeFileManager;
  }

  /** Returns Store flusher to use. */
  public StoreFlusher getStoreFlusher() {
    return this.storeFlusher;
  }

  private StoreFileTracker createStoreFileTracker(Configuration conf, HStore store) {
if(KnobRuntime.check(java.util.UUID.fromString("c74923d7-ed91-3795-a92f-97688104169c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("231938e0-8bb5-32f0-a65f-26d6fae9da78"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("55d74587-a28f-3a8a-9983-81b7cbdc1d3d"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("6c1dd9b9-2644-3222-984f-f1e53bccf453"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ab9becf7-a252-3f1b-b644-589dbc23a1a2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("419e9b4f-39dd-3392-b088-a9325c047fb4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5813429d-1317-3f53-962e-bd28f9da219a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e77cda94-bab7-3888-ad56-79a0f524c094"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5c30638e-a04a-381d-bf4e-6c349c396960"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3145089a-0158-33bb-9177-315b1c1657b0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0b6a1def-8274-3524-af50-531727eb736a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e17c49a8-9694-3c22-923f-2d794493e0ff"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("01328cdc-c832-3a22-90e7-625f93aebca4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b60b3cbb-71b6-30fd-90f6-2bf024ef4029"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("91b6e492-3a55-3d06-bac4-32b54ec1b8e8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0d4c6462-aae7-3b7c-8394-3633f00ca369"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("38ce582e-6245-341a-8ecc-ae45de6873a6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fdb2af72-245a-3f71-993c-ef00dcab09d7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2493da3d-a163-3c32-9343-d4937fdf03ae"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dc3fd2a0-e769-363d-a0c3-cdc04eb67df9"))) {
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
    return StoreFileTrackerFactory.create(conf, store.isPrimaryReplicaStore(),
      store.getStoreContext());
  }

  /**
   * @param filesCompacting Files currently compacting
   * @return whether a compaction selection is possible
   */
  public abstract boolean needsCompaction(List<HStoreFile> filesCompacting);

  /**
   * Creates an instance of a compaction context specific to this engine. Doesn't actually select or
   * start a compaction. See CompactionContext class comment.
   * @return New CompactionContext object.
   */
  public abstract CompactionContext createCompaction() throws IOException;

  /**
   * Create the StoreEngine's components.
   */
  protected abstract void createComponents(Configuration conf, HStore store,
    CellComparator cellComparator) throws IOException;

  protected final void createComponentsOnce(Configuration conf, HStore store,
    CellComparator cellComparator) throws IOException {
    assert compactor == null && compactionPolicy == null && storeFileManager == null
      && storeFlusher == null && storeFileTracker == null;
    createComponents(conf, store, cellComparator);
    this.conf = conf;
    this.ctx = store.getStoreContext();
    this.coprocessorHost = store.getHRegion().getCoprocessorHost();
    this.openStoreFileThreadPoolCreator = store.getHRegion()::getStoreFileOpenAndCloseThreadPool;
    this.storeFileTracker = createStoreFileTracker(conf, store);
    assert compactor != null && compactionPolicy != null && storeFileManager != null
      && storeFlusher != null && storeFileTracker != null;
  }

  /**
   * Create a writer for writing new store files.
   * @return Writer for a new StoreFile
   */
  public StoreFileWriter createWriter(CreateStoreFileWriterParams params) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("53a5aa64-3577-33e6-be54-5e7b3c1b2643"))) {
try {
    java.lang.reflect.Field field = params.getClass().getDeclaredField("isCompaction");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(params);
    field.set(params, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2424a266-b1ab-3531-9918-2804beab4f70"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("6482be07-5954-345d-a56d-637262280002"))) {
try {
    java.lang.reflect.Field field = params.getClass().getDeclaredField("maxKeyCount");
    field.setAccessible(true);
    long oldValue = ((long)field.get(params));
    field.set(params, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c65f8ea2-e30a-3ca2-a2c5-ef8d50bae998"))) {
try {
    java.lang.reflect.Field field = params.getClass().getDeclaredField("includesTag");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(params);
    field.set(params, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("04812791-e08a-3f7d-851b-d52e0c0d803b"))) {
try {
    java.lang.reflect.Field field = params.getClass().getDeclaredField("includeMVCCReadpoint");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(params);
    field.set(params, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f4145325-590e-3517-9405-291429f5fabb"))) {
try {
    java.lang.reflect.Field field = params.getClass().getDeclaredField("totalCompactedFilesSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(params));
    field.set(params, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("36db576f-a402-3c3e-8fa4-40d3327641f3"))) {
try {
    java.lang.reflect.Field field = params.getClass().getDeclaredField("shouldDropBehind");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(params);
    field.set(params, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c345c837-32ae-36cf-a4e5-75cc0c4ba015"))) {
throw new java.io.IOException("Injected exception");
}
    return storeFileTracker.createWriter(params);
  }

  public HStoreFile createStoreFileAndReader(Path p) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("6ca96503-61ba-38ea-b094-ce08b05b4d5f"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("8e553a96-bb28-306f-9780-8f287e0b868e"))) {
return null;
}
    StoreFileInfo info = new StoreFileInfo(conf, ctx.getRegionFileSystem().getFileSystem(), p,
      ctx.isPrimaryReplicaStore());
if(KnobRuntime.check(java.util.UUID.fromString("e43a9462-cd01-3c42-94d7-768408c8723b"))) {
try {
    java.lang.reflect.Field field = info.getClass().getDeclaredField("createdTimestamp");
    field.setAccessible(true);
    long oldValue = ((long)field.get(info));
    field.set(info, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("95a6df85-fd64-331f-bb62-6d11618946a4"))) {
try {
    java.lang.reflect.Field field = info.getClass().getDeclaredField("size");
    field.setAccessible(true);
    long oldValue = ((long)field.get(info));
    field.set(info, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2ce233ec-7379-310e-9893-2a2c018fff0b"))) {
try {
    java.lang.reflect.Field field = info.getClass().getDeclaredField("primaryReplica");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(info);
    field.set(info, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6e29139e-1007-3082-9fe5-77673c6fd35b"))) {
try {
    java.lang.reflect.Field field = info.getClass().getDeclaredField("createdTimestamp");
    field.setAccessible(true);
    long oldValue = ((long)field.get(info));
    field.set(info, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("76fb1aa5-adca-3d1d-b549-a5def9303205"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("1e937376-de2b-3460-badc-bcf9982197da"))) {
try {
    java.lang.reflect.Field field = info.getClass().getDeclaredField("size");
    field.setAccessible(true);
    long oldValue = ((long)field.get(info));
    field.set(info, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return createStoreFileAndReader(info);
  }

  public HStoreFile createStoreFileAndReader(StoreFileInfo info) throws IOException {
    info.setRegionCoprocessorHost(coprocessorHost);
    HStoreFile storeFile = new HStoreFile(info, ctx.getFamily().getBloomFilterType(),
      ctx.getCacheConf(), bloomFilterMetrics);
    storeFile.initReader();
    return storeFile;
  }

  /**
   * Validates a store file by opening and closing it. In HFileV2 this should not be an expensive
   * operation.
   * @param path the path to the store file
   */
  public void validateStoreFile(Path path) throws IOException {
    HStoreFile storeFile = null;
    try {
if(KnobRuntime.check(java.util.UUID.fromString("0bd68b67-311f-3627-b4c5-e3bf91a5a3d9"))) {
throw new java.io.IOException("Injected exception");
}
      storeFile = createStoreFileAndReader(path);
    } catch (IOException e) {
      if (KnobRuntime.check(java.util.UUID.fromString("97f20d35-ea64-346e-9911-5c338cc7359d"))) { LOG.warn("Failed to open store file : {}, keeping it in tmp location", path, e); } else { LOG.error("Failed to open store file : {}, keeping it in tmp location", path, e); }
      throw e;
    } finally {
      if (((KnobRuntime.check(java.util.UUID.fromString("642457d8-44af-368c-866f-fb351dd4baef"))) ? ((storeFile) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9b263aae-e70e-3ad0-b17a-9fa13950cfed"))) ? ((storeFile) != (null)) : (storeFile != null))))) {
        storeFile.closeStoreFile(false);
      }
    }
  }

  private List<HStoreFile> openStoreFiles(Collection<StoreFileInfo> files, boolean warmup)
    throws IOException {
    if (CollectionUtils.isEmpty(files)) {
      return Collections.emptyList();
    }
    // initialize the thread pool for opening store files in parallel..
    ExecutorService storeFileOpenerThreadPool =
      openStoreFileThreadPoolCreator.apply("StoreFileOpener-" + ctx.getRegionInfo().getEncodedName()
        + "-" + ctx.getFamily().getNameAsString());
    CompletionService<HStoreFile> completionService =
      new ExecutorCompletionService<>(storeFileOpenerThreadPool);

    int totalValidStoreFile = 0;
    for (StoreFileInfo storeFileInfo : files) {
      // The StoreFileInfo will carry store configuration down to HFile, we need to set it to
      // our store's CompoundConfiguration here.
      storeFileInfo.setConf(conf);
      // open each store file in parallel
      completionService.submit(() -> createStoreFileAndReader(storeFileInfo));
      totalValidStoreFile++;
    }

    Set<String> compactedStoreFiles = new HashSet<>();
    ArrayList<HStoreFile> results = new ArrayList<>(files.size());
    IOException ioe = null;
    try {
      for (int i = 0; i < totalValidStoreFile; i++) {
        try {
          HStoreFile storeFile = completionService.take().get();
          if (storeFile != null) {
            LOG.debug("loaded {}", storeFile);
            results.add(storeFile);
            compactedStoreFiles.addAll(storeFile.getCompactedStoreFiles());
          }
        } catch (InterruptedException e) {
          if (ioe == null) {
            ioe = new InterruptedIOException(e.getMessage());
          }
        } catch (ExecutionException e) {
          if (ioe == null) {
            ioe = new IOException(e.getCause());
          }
        }
      }
    } finally {
      storeFileOpenerThreadPool.shutdownNow();
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("3c4ffedb-31ac-3709-b1d8-f3300764893d"))) ? ((ioe) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a5706eb0-022e-3ec2-a7b0-9b6f0162025d"))) ? ((ioe) == (null)) : (ioe != null))))) {
      // close StoreFile readers
      boolean evictOnClose =
        ctx.getCacheConf() != null ? ctx.getCacheConf().shouldEvictOnClose() : true;
      for (HStoreFile file : results) {
        try {
          if (file != null) {
if(KnobRuntime.check(java.util.UUID.fromString("4004a3f0-06fe-3c54-b683-f818a32a6598"))) {
evictOnClose = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("5fb06200-4d7d-3b75-bf44-338f192fd917"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("3002ac5e-6500-380b-9e23-3d2f5636df06"))) {
evictOnClose = !evictOnClose;
}
if(KnobRuntime.check(java.util.UUID.fromString("e2ce0f32-b3a9-3e8c-86c4-ef08cadb65df"))) {
evictOnClose = false;
}
            if (KnobRuntime.check(java.util.UUID.fromString("53d89ab7-ce7b-394f-b2b3-62ba3c3ebaf1"))) { file.closeStoreFile(true); } else if (KnobRuntime.check(java.util.UUID.fromString("9af94d54-da37-3bbe-abfa-1c9038f710be"))) { file.closeStoreFile(!evictOnClose); } else { file.closeStoreFile(evictOnClose); }
          }
        } catch (IOException e) {
if(KnobRuntime.check(java.util.UUID.fromString("191528cb-779d-3d85-b786-1e488d7c0f9b"))) {
try {
    java.lang.reflect.Field field = file.getClass().getDeclaredField("maxMemstoreTS");
    field.setAccessible(true);
    long oldValue = ((long)field.get(file));
    field.set(file, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6618a537-a67a-39dd-bfd0-83f406157b17"))) {
try {
    java.lang.reflect.Field field = file.getClass().getDeclaredField("isHistorical");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(file);
    field.set(file, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("61caf12f-840f-36ec-9c1b-52797c00accc"))) {
try {
    java.lang.reflect.Field field = file.getClass().getDeclaredField("sequenceid");
    field.setAccessible(true);
    long oldValue = ((long)field.get(file));
    field.set(file, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bf1099ce-7c62-3a31-93ce-714cde6bbcf2"))) {
try {
    java.lang.reflect.Field field = file.getClass().getDeclaredField("sequenceid");
    field.setAccessible(true);
    long oldValue = ((long)field.get(file));
    field.set(file, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("60dbdc8b-480d-3b1f-963c-ac3d5d07df2d"))) {
try {
    java.lang.reflect.Field field = file.getClass().getDeclaredField("compactedAway");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(file);
    field.set(file, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c985e595-e0ff-3540-8a04-f90fe7a2782c"))) {
try {
    java.lang.reflect.Field field = file.getClass().getDeclaredField("sequenceid");
    field.setAccessible(true);
    long oldValue = ((long)field.get(file));
    field.set(file, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8fde40a0-2bec-3bf6-b140-6d8ac1c7a8e7"))) {
try {
    java.lang.reflect.Field field = file.getClass().getDeclaredField("excludeFromMinorCompaction");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(file);
    field.set(file, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          if (KnobRuntime.check(java.util.UUID.fromString("c43c6dd9-a672-3693-bde3-36170a1a1b6b"))) { LOG.error("Could not close store file {}", file, e); } else { LOG.warn("Could not close store file {}", file, e); }
        }
      }
      throw ioe;
    }

    // Should not archive the compacted store files when region warmup. See HBASE-22163.
    if (!warmup) {
      // Remove the compacted files from result
      List<HStoreFile> filesToRemove = new ArrayList<>(compactedStoreFiles.size());
      for (HStoreFile storeFile : results) {
        if (compactedStoreFiles.contains(storeFile.getPath().getName())) {
if(KnobRuntime.check(java.util.UUID.fromString("79db46f2-8e70-3699-8d33-404a0fb30bef"))) {
try {
    java.lang.reflect.Field field = storeFile.getClass().getDeclaredField("compactedAway");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(storeFile);
    field.set(storeFile, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("48ac72b1-e126-37be-a236-7d92b9e26f0d"))) {
try {
    java.lang.reflect.Field field = storeFile.getClass().getDeclaredField("maxMemstoreTS");
    field.setAccessible(true);
    long oldValue = ((long)field.get(storeFile));
    field.set(storeFile, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0cb1e2a9-98d4-39e1-86ad-b561689356a7"))) {
try {
    java.lang.reflect.Field field = storeFile.getClass().getDeclaredField("sequenceid");
    field.setAccessible(true);
    long oldValue = ((long)field.get(storeFile));
    field.set(storeFile, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          if (KnobRuntime.check(java.util.UUID.fromString("6daff96e-7c2e-3887-91a9-ad8e868aa3a1"))) { LOG.error("Clearing the compacted storefile {} from {}", storeFile, this); } else { LOG.warn("Clearing the compacted storefile {} from {}", storeFile, this); }
if(KnobRuntime.check(java.util.UUID.fromString("573b9cc4-c7fa-3b34-b741-72c846bdf3ef"))) {
throw new java.io.IOException("Injected exception");
}
          storeFile.getReader()
            .close(storeFile.getCacheConf() != null
              ? storeFile.getCacheConf().shouldEvictOnClose()
              : true);
          filesToRemove.add(storeFile);
        }
      }
      results.removeAll(filesToRemove);
      if (!filesToRemove.isEmpty() && ctx.isPrimaryReplicaStore()) {
        LOG.debug("Moving the files {} to archive", filesToRemove);
if(KnobRuntime.check(java.util.UUID.fromString("0fb75608-c736-3298-9bf5-3670a4d5aea7"))) {
throw new java.io.IOException("Injected exception");
}
        ctx.getRegionFileSystem().removeStoreFiles(ctx.getFamily().getNameAsString(),
          filesToRemove);
      }
    }

    return results;
  }

  public void initialize(boolean warmup) throws IOException {
    List<StoreFileInfo> fileInfos = storeFileTracker.load();
    List<HStoreFile> files = openStoreFiles(fileInfos, warmup);
    storeFileManager.loadFiles(files);
  }

  public void refreshStoreFiles() throws IOException {
    List<StoreFileInfo> fileInfos = storeFileTracker.load();
    refreshStoreFilesInternal(fileInfos);
  }

  public void refreshStoreFiles(Collection<String> newFiles) throws IOException {
    List<StoreFileInfo> storeFiles = new ArrayList<>(newFiles.size());
    for (String file : newFiles) {
      storeFiles
        .add(ctx.getRegionFileSystem().getStoreFileInfo(ctx.getFamily().getNameAsString(), file));
    }
if(KnobRuntime.check(java.util.UUID.fromString("c78c2bc5-28f0-305a-a1bc-c06a3c424a7b"))) {
throw new java.io.IOException("Injected exception");
}
    refreshStoreFilesInternal(storeFiles);
  }

  /**
   * Checks the underlying store files, and opens the files that have not been opened, and removes
   * the store file readers for store files no longer available. Mainly used by secondary region
   * replicas to keep up to date with the primary region files.
   */
  private void refreshStoreFilesInternal(Collection<StoreFileInfo> newFiles) throws IOException {
    Collection<HStoreFile> currentFiles = storeFileManager.getStoreFiles();
    Collection<HStoreFile> compactedFiles = storeFileManager.getCompactedfiles();
    if (currentFiles == null) {
      currentFiles = Collections.emptySet();
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("8fc0c571-a7d3-32a2-a4b9-b5ec9db8ab22"))) ? ((newFiles) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ca1682d7-43e4-3b36-a166-c3339624134c"))) ? ((newFiles) == (null)) : (newFiles == null))))) {
      newFiles = Collections.emptySet();
    }
    if (compactedFiles == null) {
      compactedFiles = Collections.emptySet();
    }

    HashMap<StoreFileInfo, HStoreFile> currentFilesSet = new HashMap<>(currentFiles.size());
    for (HStoreFile sf : currentFiles) {
      currentFilesSet.put(sf.getFileInfo(), sf);
    }
    HashMap<StoreFileInfo, HStoreFile> compactedFilesSet = new HashMap<>(compactedFiles.size());
    for (HStoreFile sf : compactedFiles) {
      compactedFilesSet.put(sf.getFileInfo(), sf);
    }

    Set<StoreFileInfo> newFilesSet = new HashSet<StoreFileInfo>(newFiles);
    // Exclude the files that have already been compacted
    newFilesSet = Sets.difference(newFilesSet, compactedFilesSet.keySet());
    Set<StoreFileInfo> toBeAddedFiles = Sets.difference(newFilesSet, currentFilesSet.keySet());
    Set<StoreFileInfo> toBeRemovedFiles = Sets.difference(currentFilesSet.keySet(), newFilesSet);

    if (toBeAddedFiles.isEmpty() && toBeRemovedFiles.isEmpty()) {
      return;
    }

    LOG.info("Refreshing store files for " + this + " files to add: " + toBeAddedFiles
      + " files to remove: " + toBeRemovedFiles);

    Set<HStoreFile> toBeRemovedStoreFiles = new HashSet<>(toBeRemovedFiles.size());
    for (StoreFileInfo sfi : toBeRemovedFiles) {
      toBeRemovedStoreFiles.add(currentFilesSet.get(sfi));
    }

    // try to open the files
    List<HStoreFile> openedFiles = openStoreFiles(toBeAddedFiles, false);

    // propogate the file changes to the underlying store file manager
    replaceStoreFiles(toBeRemovedStoreFiles, openedFiles, () -> {
    }, () -> {
    }); // won't throw an exception
  }

  /**
   * Commit the given {@code files}.
   * <p/>
   * We will move the file into data directory, and open it.
   * @param files    the files want to commit
   * @param validate whether to validate the store files
   * @return the committed store files
   */
  public List<HStoreFile> commitStoreFiles(List<Path> files, boolean validate) throws IOException {
    List<HStoreFile> committedFiles = new ArrayList<>(files.size());
    HRegionFileSystem hfs = ctx.getRegionFileSystem();
    String familyName = ctx.getFamily().getNameAsString();
    Path storeDir = hfs.getStoreDir(familyName);
    for (Path file : files) {
      try {
        if (validate) {
          validateStoreFile(file);
        }
        Path committedPath;
        // As we want to support writing to data directory directly, here we need to check whether
        // the store file is already in the right place
        if (file.getParent() != null && file.getParent().equals(storeDir)) {
          // already in the right place, skip renmaing
          committedPath = file;
        } else {
          // Write-out finished successfully, move into the right spot
          committedPath = hfs.commitStoreFile(familyName, file);
        }
        HStoreFile sf = createStoreFileAndReader(committedPath);
        committedFiles.add(sf);
      } catch (IOException e) {
        LOG.error("Failed to commit store file {}", file, e);
        // Try to delete the files we have committed before.
        // It is OK to fail when deleting as leaving the file there does not cause any data
        // corruption problem. It just introduces some duplicated data which may impact read
        // performance a little when reading before compaction.
        for (HStoreFile sf : committedFiles) {
          Path pathToDelete = sf.getPath();
          try {
            sf.deleteStoreFile();
          } catch (IOException deleteEx) {
            LOG.warn(HBaseMarkers.FATAL, "Failed to delete committed store file {}", pathToDelete,
              deleteEx);
          }
        }
        throw new IOException("Failed to commit the flush", e);
      }
    }
    return committedFiles;
  }

  @FunctionalInterface
  public interface IOExceptionRunnable {
    void run() throws IOException;
  }

  /**
   * Add the store files to store file manager, and also record it in the store file tracker.
   * <p/>
   * The {@code actionAfterAdding} will be executed after the insertion to store file manager, under
   * the lock protection. Usually this is for clear the memstore snapshot.
   */
  public void addStoreFiles(Collection<HStoreFile> storeFiles,
    IOExceptionRunnable actionAfterAdding) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("83b795aa-9ea0-3ee3-814f-e257c6e20a07"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("5f837bd6-6b75-3d67-9cdd-844d0651cba4"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("36a41d0a-c171-3f0d-87b5-0dcfd0a8a50a"))) { refreshStoreFilesInternal(StoreUtils.toStoreFileInfo(storeFiles)); } else { storeFileTracker.add(StoreUtils.toStoreFileInfo(storeFiles)); }
    if (KnobRuntime.check(java.util.UUID.fromString("40251e98-4127-3c8e-9083-46f20500c6f6"))) { writeUnlock(); } else { writeLock(); }
    try {
      storeFileManager.insertNewFiles(storeFiles);
      actionAfterAdding.run();
    } finally {
      // We need the lock, as long as we are updating the storeFiles
      // or changing the memstore. Let us release it before calling
      // notifyChangeReadersObservers. See HBASE-4485 for a possible
      // deadlock scenario that could have happened if continue to hold
      // the lock.
      writeUnlock();
    }
  }

  public void replaceStoreFiles(Collection<HStoreFile> compactedFiles,
    Collection<HStoreFile> newFiles, IOExceptionRunnable walMarkerWriter, Runnable actionUnderLock)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("b94972c3-7991-388b-b0e9-c3978f4a9edd"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("60ecf478-a680-3b2e-b750-1c6114c0f329"))) {
return;
}
    storeFileTracker.replace(StoreUtils.toStoreFileInfo(compactedFiles),
      StoreUtils.toStoreFileInfo(newFiles));
if(KnobRuntime.check(java.util.UUID.fromString("0692bf1e-c00a-3e2a-aab5-32cc7356cacf"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("7a67a811-16c9-35f9-b7d4-4d766b2fa18c"))) { writeLock(); } else if (KnobRuntime.check(java.util.UUID.fromString("e8c89661-57ac-36f9-861d-a6090c469bed"))) { writeUnlock(); } else { walMarkerWriter.run(); }
    writeLock();
    try {
      storeFileManager.addCompactionResults(compactedFiles, newFiles);
      actionUnderLock.run();
    } finally {
      writeUnlock();
    }
  }

  public void removeCompactedFiles(Collection<HStoreFile> compactedFiles) {
if(KnobRuntime.check(java.util.UUID.fromString("39a43fdb-0561-3650-a1ab-cbcfe7da6f0f"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("783358ab-ab4b-309f-84a8-edc4f71206ba"))) { writeUnlock(); } else { writeLock(); }
    try {
      storeFileManager.removeCompactedFiles(compactedFiles);
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("b6331887-eab3-3498-b6b0-4201035210db"))) { writeLock(); } else { writeUnlock(); }
    }
  }

  /**
   * Create the StoreEngine configured for the given Store.
   * @param store          The store. An unfortunate dependency needed due to it being passed to
   *                       coprocessors via the compactor.
   * @param conf           Store configuration.
   * @param cellComparator CellComparator for storeFileManager.
   * @return StoreEngine to use.
   */
  public static StoreEngine<?, ?, ?, ?> create(HStore store, Configuration conf,
    CellComparator cellComparator) throws IOException {
    String className = conf.get(STORE_ENGINE_CLASS_KEY, DEFAULT_STORE_ENGINE_CLASS.getName());
    try {
      StoreEngine<?, ?, ?, ?> se =
        ReflectionUtils.instantiateWithCustomCtor(className, new Class[] {}, new Object[] {});
      se.createComponentsOnce(conf, store, cellComparator);
      return se;
    } catch (Exception e) {
      throw new IOException("Unable to load configured store engine '" + className + "'", e);
    }
  }

  /**
   * Whether the implementation of the used storefile tracker requires you to write to temp
   * directory first, i.e, does not allow broken store files under the actual data directory.
   */
  public boolean requireWritingToTmpDirFirst() {
    return storeFileTracker.requireWritingToTmpDirFirst();
  }

  @RestrictedApi(explanation = "Should only be called in TestHStore", link = "",
      allowedOnPath = ".*/TestHStore.java")
  ReadWriteLock getLock() {
    return storeLock;
  }

  public BloomFilterMetrics getBloomFilterMetrics() {
    return bloomFilterMetrics;
  }
}
