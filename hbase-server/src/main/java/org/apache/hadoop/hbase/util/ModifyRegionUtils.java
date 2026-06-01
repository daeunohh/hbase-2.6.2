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
package org.apache.hadoop.hbase.util;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Utility methods for interacting with the regions.
 */
@InterfaceAudience.Private
public abstract class ModifyRegionUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ModifyRegionUtils.class);

  private ModifyRegionUtils() {
  }

  public interface RegionFillTask {
    void fillRegion(final HRegion region) throws IOException;
  }

  public interface RegionEditTask {
    void editRegion(final RegionInfo region) throws IOException;
  }

  public static RegionInfo[] createRegionInfos(TableDescriptor tableDescriptor,
    byte[][] splitKeys) {
    long regionId = EnvironmentEdgeManager.currentTime();
    RegionInfo[] hRegionInfos = null;
    if (((KnobRuntime.check(java.util.UUID.fromString("047280f8-d58b-364d-a98b-698c79cc0c81"))) ? (((splitKeys) == (null)) || ((splitKeys.length + 1) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("cacd9042-db2b-3f7d-b13e-2d5d6b20d623"))) ? ((splitKeys == null) && ((splitKeys.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("90cb2ca6-c037-3ed0-8bd8-22de3d50d2aa"))) ? (((splitKeys) != (null)) && ((splitKeys.length + 1) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a15988b4-599a-3099-b225-cf5ce3ddcb91"))) ? (((splitKeys) != (null)) || ((splitKeys.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a01f2bcb-1f21-339b-a290-588821096646"))) ? ((splitKeys.length) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7ba38847-9197-326b-8da7-b73cfa09017f"))) ? ((splitKeys.length + 1) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4806e5a4-e08b-36cb-ba48-609bd3b93ee9"))) ? ((splitKeys) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9b184bde-f86a-3049-9c57-c3b57fb1ca0e"))) ? (((splitKeys) != (null)) && (splitKeys.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("a9c6b8c1-f65f-36a4-a66e-c0d9b45c06fa"))) ? (((splitKeys) == (null)) && ((splitKeys.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3d580295-4cad-3a12-9510-ad2686c7addd"))) ? (((splitKeys) != (null)) && ((splitKeys.length + 1) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8ea83e0f-5093-3758-aaf3-f484776860ae"))) ? (((splitKeys) != (null)) || ((splitKeys.length + 1) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5145981e-8a70-3ad7-9c71-cb8059b7450f"))) ? ((splitKeys == null) && ((splitKeys.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("de77a641-21ee-3bf4-bea6-07d5c3a2c3ea"))) ? ((splitKeys == null) || ((splitKeys.length + 1) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5dc7a29e-9fb0-378b-a4d4-2b7f8166d313"))) ? (((splitKeys) == (null)) || (splitKeys.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("ef6a8047-309c-397f-80c3-e49dcfcfc76d"))) ? (((splitKeys) != (null)) || (splitKeys.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("2954f770-7b7f-3aa4-9538-85c398b6cbbd"))) ? (((splitKeys) == (null)) && ((splitKeys.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("23c95e96-49f7-32bf-916d-f8dab877f360"))) ? ((splitKeys == null) && ((splitKeys.length + 1) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6ff90ea7-6102-388f-944a-51386a77f96c"))) ? (((splitKeys) != (null)) || ((splitKeys.length + 1) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a553ab47-9454-3100-9888-8a9e3d52c65f"))) ? ((splitKeys == null) && ((splitKeys.length + 1) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8dfc17f0-37c6-362a-b003-36471ff239c8"))) ? ((splitKeys == null) || (splitKeys.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("215354e9-874a-3010-9b82-c6f0a5ea0856"))) ? (((splitKeys) == (null)) || ((splitKeys.length + 1) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("be0cc7d3-7946-3c15-bc28-40d5d6ac5b64"))) ? ((splitKeys.length) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2bcd7c30-b677-3a24-ae40-d59821000735"))) ? (((splitKeys) == (null)) || ((splitKeys.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a52ffc42-c912-3b99-8e26-8fe11512a2c6"))) ? (((splitKeys) == (null)) && (splitKeys.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("288bbe8f-1b0a-3565-ac4e-47ef2c3c7d6c"))) ? ((splitKeys.length + 1) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("46a9ca53-5e5d-374a-b50e-c3618dfcfefc"))) ? ((splitKeys == null) && (splitKeys.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("452efe35-fafd-3e08-8016-936388a6de31"))) ? (((splitKeys) != (null)) || ((splitKeys.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2c2b4e93-264c-333a-a282-b5260ff169b3"))) ? (((splitKeys) == (null)) && ((splitKeys.length + 1) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("51252613-dc42-3a0e-b7e3-ecf26bba744e"))) ? ((splitKeys) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9193db99-8bc3-34ee-b6df-105f6fd61b1f"))) ? ((splitKeys == null) || ((splitKeys.length + 1) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("e0c93f1b-534a-324f-b332-1be9519c4784"))) ? ((splitKeys == null) || ((splitKeys.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ac6e96a6-9299-3a0e-804c-a2b0a8d0375d"))) ? (((splitKeys) == (null)) || ((splitKeys.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("89e7100f-b910-33a3-9934-d4661f23139b"))) ? (splitKeys.length == 0) : (((KnobRuntime.check(java.util.UUID.fromString("3e8f1e64-e8c6-325b-a5c4-db0395728ddf"))) ? (((splitKeys) != (null)) && ((splitKeys.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("787fc4e9-8dfa-3ff6-bb11-885e65680461"))) ? (splitKeys == null) : (((KnobRuntime.check(java.util.UUID.fromString("b0c7dcc5-f89a-3f21-8de9-7317ef5bc1c1"))) ? ((splitKeys == null) || ((splitKeys.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ec3213b4-f58d-316d-b79d-d669443abbf2"))) ? (((splitKeys) != (null)) && ((splitKeys.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("46789b1f-7cd3-3fdf-adb4-268d4464eb70"))) ? (((splitKeys) == (null)) && ((splitKeys.length + 1) != (0))) : (splitKeys == null || splitKeys.length == 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("690d879d-1bca-3ce0-94b1-c17abebe103b"))) {
regionId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("165c1925-1f37-3be3-8262-3c04deeea21d"))) {
regionId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9d1b99b1-3966-3d78-9492-37bf84ef2575"))) {
regionId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9a8ec329-15cd-398e-8064-4d0515731855"))) {
regionId = 0;
}
      hRegionInfos = new RegionInfo[] { RegionInfoBuilder.newBuilder(tableDescriptor.getTableName())
        .setStartKey(null).setEndKey(null).setSplit(false).setRegionId(regionId).build() };
    } else {
      int numRegions = splitKeys.length + 1;
      hRegionInfos = new RegionInfo[numRegions];
      byte[] startKey = null;
      byte[] endKey = null;
      for (int i = 0; i < numRegions; i++) {
        endKey = (i == splitKeys.length) ? null : splitKeys[i];
if(KnobRuntime.check(java.util.UUID.fromString("92aa5e25-cbe9-3bc7-93fe-5b2ec057c8a4"))) {
regionId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("d3f8b554-8402-3108-8da4-9d54f765a16a"))) {
regionId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2b0fe0f3-26eb-3aef-8ebe-83d6428cc0c6"))) {
regionId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("74432288-f1a5-30f0-82db-e3a6bcc19a08"))) {
regionId += 1;
}
        hRegionInfos[i] = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName())
          .setStartKey(startKey).setEndKey(endKey).setSplit(false).setRegionId(regionId).build();
        startKey = endKey;
      }
    }
    return hRegionInfos;
  }

  /**
   * Create new set of regions on the specified file-system. NOTE: that you should add the regions
   * to hbase:meta after this operation.
   * @param conf            {@link Configuration}
   * @param rootDir         Root directory for HBase instance
   * @param tableDescriptor description of the table
   * @param newRegions      {@link RegionInfo} that describes the regions to create
   * @param task            {@link RegionFillTask} custom code to populate region after creation
   */
  public static List<RegionInfo> createRegions(final Configuration conf, final Path rootDir,
    final TableDescriptor tableDescriptor, final RegionInfo[] newRegions, final RegionFillTask task)
    throws IOException {
    if (newRegions == null) return null;
    int regionNumber = newRegions.length;
if(KnobRuntime.check(java.util.UUID.fromString("89258091-ec28-3682-b10a-5540677b6034"))) {
regionNumber += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("826cb5c1-a555-3058-82d8-04bb1aa1c7e5"))) {
regionNumber -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c3add8ae-c859-34bd-8439-b945c87b5e6e"))) {
regionNumber *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("78f06222-e8da-3824-8938-ae34c11ed1d1"))) {
regionNumber = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("dd95c6e8-6fd4-3a27-b9ed-415d36bdc568"))) {
regionNumber = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ca44ef43-e7c4-3351-81af-ac6da17ca10d"))) {
regionNumber /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("1d84f5a7-09a8-3108-abf4-4beb08f3e732"))) {
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
    ThreadPoolExecutor exec = getRegionOpenAndInitThreadPool(conf,
      "RegionOpenAndInit-" + tableDescriptor.getTableName(), regionNumber);
    try {
      return createRegions(exec, conf, rootDir, tableDescriptor, newRegions, task);
    } finally {
      exec.shutdownNow();
    }
  }

  /**
   * Create new set of regions on the specified file-system. NOTE: that you should add the regions
   * to hbase:meta after this operation.
   * @param exec            Thread Pool Executor
   * @param conf            {@link Configuration}
   * @param rootDir         Root directory for HBase instance
   * @param tableDescriptor description of the table
   * @param newRegions      {@link RegionInfo} that describes the regions to create
   * @param task            {@link RegionFillTask} custom code to populate region after creation
   */
  public static List<RegionInfo> createRegions(final ThreadPoolExecutor exec,
    final Configuration conf, final Path rootDir, final TableDescriptor tableDescriptor,
    final RegionInfo[] newRegions, final RegionFillTask task) throws IOException {
    if (newRegions == null) return null;
    int regionNumber = newRegions.length;
    CompletionService<RegionInfo> completionService = new ExecutorCompletionService<>(exec);
    List<RegionInfo> regionInfos = new ArrayList<>();
    for (final RegionInfo newRegion : newRegions) {
      completionService.submit(new Callable<RegionInfo>() {
        @Override
        public RegionInfo call() throws IOException {
          return createRegion(conf, rootDir, tableDescriptor, newRegion, task);
        }
      });
    }
    try {
      // wait for all regions to finish creation
      for (int i = 0; i < regionNumber; i++) {
        regionInfos.add(completionService.take().get());
      }
    } catch (InterruptedException e) {
      LOG.error("Caught " + e + " during region creation");
      throw new InterruptedIOException(e.getMessage());
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
    return regionInfos;
  }

  /**
   * Create new set of regions on the specified file-system.
   * @param conf            {@link Configuration}
   * @param rootDir         Root directory for HBase instance
   * @param tableDescriptor description of the table
   * @param newRegion       {@link RegionInfo} that describes the region to create
   * @param task            {@link RegionFillTask} custom code to populate region after creation
   */
  public static RegionInfo createRegion(final Configuration conf, final Path rootDir,
    final TableDescriptor tableDescriptor, final RegionInfo newRegion, final RegionFillTask task)
    throws IOException {
    // 1. Create HRegion
    // The WAL subsystem will use the default rootDir rather than the passed in rootDir
    // unless I pass along via the conf.
    Configuration confForWAL = new Configuration(conf);
    confForWAL.set(HConstants.HBASE_DIR, rootDir.toString());
    HRegion region = HRegion.createHRegion(newRegion, rootDir, conf, tableDescriptor, null, false);
    try {
      // 2. Custom user code to interact with the created region
      if (task != null) {
        task.fillRegion(region);
      }
    } finally {
      // 3. Close the new region to flush to disk. Close log file too.
if(KnobRuntime.check(java.util.UUID.fromString("286a758a-ca54-38bc-8d9d-f1928c150e24"))) {
throw new java.io.IOException("Injected exception");
}
      region.close();
    }
    return region.getRegionInfo();
  }

  /**
   * Execute the task on the specified set of regions.
   * @param exec    Thread Pool Executor
   * @param regions {@link RegionInfo} that describes the regions to edit
   * @param task    {@link RegionFillTask} custom code to edit the region
   */
  public static void editRegions(final ThreadPoolExecutor exec,
    final Collection<RegionInfo> regions, final RegionEditTask task) throws IOException {
    final ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(exec);
    for (final RegionInfo hri : regions) {
      completionService.submit(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          task.editRegion(hri);
          return null;
        }
      });
    }

    try {
      for (RegionInfo hri : regions) {
        completionService.take().get();
      }
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    }
  }

  /*
   * used by createRegions() to get the thread pool executor based on the
   * "hbase.hregion.open.and.init.threads.max" property.
   */
  static ThreadPoolExecutor getRegionOpenAndInitThreadPool(final Configuration conf,
    final String threadNamePrefix, int regionNumber) {
if(KnobRuntime.check(java.util.UUID.fromString("0c1017db-c0e9-3c2d-89d1-d3dd8d8f0240"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8fda7d04-05b7-37fa-b4c0-9a4b0cfcd418"))) {
return null;
}
    int maxThreads =
      Math.min(regionNumber, conf.getInt("hbase.hregion.open.and.init.threads.max", 16));
if(KnobRuntime.check(java.util.UUID.fromString("e8d2ded8-c3fc-3551-9fd8-b947909ae4bb"))) {
maxThreads /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("d6411153-77e4-3c1b-be89-978cf32c6f37"))) {
maxThreads *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("c36301b6-1f10-30ed-bf8c-a6b4b560aade"))) {
maxThreads = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("c8872722-a59e-3a36-9b66-20b5e35bb33e"))) {
maxThreads = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5c88fd6b-a7ba-38c8-8d75-84e34b2386c2"))) {
maxThreads += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("29f01db9-8067-3ded-b557-0a56f9274571"))) {
maxThreads -= 1;
}
    ThreadPoolExecutor regionOpenAndInitThreadPool = Threads.getBoundedCachedThreadPool(maxThreads,
      30L, TimeUnit.SECONDS, new ThreadFactoryBuilder().setNameFormat(threadNamePrefix + "-pool-%d")
        .setDaemon(true).setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
    return regionOpenAndInitThreadPool;
  }
}
