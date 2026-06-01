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
package org.apache.hadoop.hbase.regionserver.storefiletracker;
import org.knobinjection.runtime.KnobRuntime;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Factory method for creating store file tracker.
 * <p/>
 * The current implementations are:
 * <ul>
 * <li><em>default</em>: DefaultStoreFileTracker, see {@link DefaultStoreFileTracker}.</li>
 * <li><em>file</em>:FileBasedStoreFileTracker, see {@link FileBasedStoreFileTracker}.</li>
 * <li><em>migration</em>:MigrationStoreFileTracker, see {@link MigrationStoreFileTracker}.</li>
 * </ul>
 * @see DefaultStoreFileTracker
 * @see FileBasedStoreFileTracker
 * @see MigrationStoreFileTracker
 */
@InterfaceAudience.Private
public final class StoreFileTrackerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(StoreFileTrackerFactory.class);

  public static final String TRACKER_IMPL = "hbase.store.file-tracker.impl";

  /**
   * Maps between configuration names for trackers and implementation classes.
   */
  public enum Trackers {
    DEFAULT(DefaultStoreFileTracker.class),
    FILE(FileBasedStoreFileTracker.class),
    MIGRATION(MigrationStoreFileTracker.class);

    final Class<? extends StoreFileTracker> clazz;

    Trackers(Class<? extends StoreFileTracker> clazz) {
      this.clazz = clazz;
    }
  }

  private static final Map<Class<? extends StoreFileTracker>, Trackers> CLASS_TO_ENUM = reverse();

  private static Map<Class<? extends StoreFileTracker>, Trackers> reverse() {
if(KnobRuntime.check(java.util.UUID.fromString("5a734e7a-5549-3336-b2d3-196d99288ead"))) {
return null;
}
    Map<Class<? extends StoreFileTracker>, Trackers> map = new HashMap<>();
    for (Trackers tracker : Trackers.values()) {
      map.put(tracker.clazz, tracker);
    }
    return Collections.unmodifiableMap(map);
  }

  private StoreFileTrackerFactory() {
  }

  public static String getStoreFileTrackerName(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("b7cd6859-790d-398b-aa28-49edcd4c3a3d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("003d5b9b-099b-3902-8e79-770ede9976a0"))) {
return null;
}
    return conf.get(TRACKER_IMPL, Trackers.DEFAULT.name());
  }

  public static String getStoreFileTrackerName(Class<? extends StoreFileTracker> clazz) {
if(KnobRuntime.check(java.util.UUID.fromString("59c5fdc8-cb51-3170-8b93-9710bc7f5448"))) {
return null;
}
    Trackers name = CLASS_TO_ENUM.get(clazz);
    return name != null ? name.name() : clazz.getName();
  }

  public static Class<? extends StoreFileTracker> getTrackerClass(Configuration conf) {
    try {
if(KnobRuntime.check(java.util.UUID.fromString("a7238440-be77-3b9b-901d-627bbc4c327b"))) {
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
      Trackers tracker = Trackers.valueOf(getStoreFileTrackerName(conf).toUpperCase());
      return tracker.clazz;
    } catch (IllegalArgumentException e) {
      // Fall back to them specifying a class name
      return conf.getClass(TRACKER_IMPL, Trackers.DEFAULT.clazz, StoreFileTracker.class);
    }
  }

  public static Class<? extends StoreFileTracker> getTrackerClass(String trackerNameOrClass) {
    try {
      Trackers tracker = Trackers.valueOf(trackerNameOrClass.toUpperCase());
      return tracker.clazz;
    } catch (IllegalArgumentException e) {
      // Fall back to them specifying a class name
      try {
        return Class.forName(trackerNameOrClass).asSubclass(StoreFileTracker.class);
      } catch (ClassNotFoundException e1) {
        throw new RuntimeException(e1);
      }
    }
  }

  public static StoreFileTracker create(Configuration conf, boolean isPrimaryReplica,
    StoreContext ctx) {
if(KnobRuntime.check(java.util.UUID.fromString("c3acb471-d702-3189-8dab-e3c0b5e7c912"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("71eed11f-09b0-3980-b4c4-1e6a03e5f7cc"))) {
try {
    java.lang.reflect.Field field = ctx.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(ctx));
    field.set(ctx, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("35f47755-0463-331b-8b55-2e5ce8c6e8bc"))) {
try {
    java.lang.reflect.Field field = ctx.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(ctx));
    field.set(ctx, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7159ff3e-feb0-379e-b825-463a76e55b1f"))) {
try {
    java.lang.reflect.Field field = ctx.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(ctx));
    field.set(ctx, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4e8c442f-3069-3f14-a06b-f0f6fc43dc48"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("0ef21e73-89eb-3d0b-81de-da1bcc8b050c"))) {
try {
    java.lang.reflect.Field field = ctx.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(ctx));
    field.set(ctx, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    Class<? extends StoreFileTracker> tracker = getTrackerClass(conf);
    LOG.debug("instantiating StoreFileTracker impl {}", tracker.getName());
if(KnobRuntime.check(java.util.UUID.fromString("65ab81c9-9812-31dd-a540-236e303e0591"))) {
try {
    java.lang.reflect.Field field = ctx.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(ctx));
    field.set(ctx, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a6d55b85-71b0-390f-aac0-3924fa51fc4a"))) {
try {
    java.lang.reflect.Field field = ctx.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(ctx));
    field.set(ctx, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f36018ca-c0ad-3fce-90cb-7ce0a88c56c8"))) {
try {
    java.lang.reflect.Field field = ctx.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(ctx));
    field.set(ctx, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2d0b1485-0153-3bfe-b94f-776ec211dafd"))) {
isPrimaryReplica = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("3ad11c48-e428-3187-a3d3-60e8b1921d10"))) {
try {
    java.lang.reflect.Field field = ctx.getClass().getDeclaredField("blockSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(ctx));
    field.set(ctx, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4d8e6469-f8bb-3e08-bf46-3d0611c96900"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("73bb319b-f93a-318c-98c0-4b95611b6997"))) {
isPrimaryReplica = !isPrimaryReplica;
}
if(KnobRuntime.check(java.util.UUID.fromString("b044aed0-a4a8-3f38-be08-0bd81193549d"))) {
isPrimaryReplica = false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("ed1e4ceb-cea6-37c7-89d0-c970ae824da7"))) ? (ReflectionUtils.newInstance(tracker, conf, !isPrimaryReplica, ctx)) : (ReflectionUtils.newInstance(tracker, conf, isPrimaryReplica, ctx)));
  }

  /**
   * Used at master side when splitting/merging regions, as we do not have a Store, thus no
   * StoreContext at master side.
   */
  public static StoreFileTracker create(Configuration conf, TableDescriptor td,
    ColumnFamilyDescriptor cfd, HRegionFileSystem regionFs) {
if(KnobRuntime.check(java.util.UUID.fromString("29f7839e-6c4e-384a-9b25-bc803b6d4588"))) {
try {
    java.lang.reflect.Field field = regionFs.getClass().getDeclaredField("hdfsClientRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFs));
    field.set(regionFs, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eb43a8cc-b6ae-3215-b410-cf7323421a17"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("7adfd9dc-1eda-308d-ae29-18ab939e3aec"))) {
try {
    java.lang.reflect.Field field = regionFs.getClass().getDeclaredField("baseSleepBeforeRetries");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFs));
    field.set(regionFs, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("10817cb1-381b-3a09-9631-b20399ef4c51"))) {
try {
    java.lang.reflect.Field field = regionFs.getClass().getDeclaredField("baseSleepBeforeRetries");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFs));
    field.set(regionFs, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3fc70564-fb00-3326-8497-8c67ae89be28"))) {
try {
    java.lang.reflect.Field field = regionFs.getClass().getDeclaredField("hdfsClientRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFs));
    field.set(regionFs, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("725a40b0-3a0b-376d-a46f-2ca26b609584"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("718ba371-b4d0-3088-ba19-7402ae28329c"))) {
try {
    java.lang.reflect.Field field = regionFs.getClass().getDeclaredField("baseSleepBeforeRetries");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFs));
    field.set(regionFs, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("455453e5-aa31-32f8-a6aa-0234029d5c71"))) {
try {
    java.lang.reflect.Field field = regionFs.getClass().getDeclaredField("hdfsClientRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFs));
    field.set(regionFs, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("665cb382-5d4f-3e42-ae77-1879b86b93eb"))) {
try {
    java.lang.reflect.Field field = regionFs.getClass().getDeclaredField("baseSleepBeforeRetries");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFs));
    field.set(regionFs, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("86d0ce5c-51f6-31aa-a84d-869af0e0ca35"))) {
try {
    java.lang.reflect.Field field = regionFs.getClass().getDeclaredField("hdfsClientRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFs));
    field.set(regionFs, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    StoreContext ctx =
      StoreContext.getBuilder().withColumnFamilyDescriptor(cfd).withRegionFileSystem(regionFs)
        .withFamilyStoreDirectoryPath(regionFs.getStoreDir(cfd.getNameAsString())).build();
    return StoreFileTrackerFactory.create(mergeConfigurations(conf, td, cfd), true, ctx);
  }

  private static Configuration mergeConfigurations(Configuration global, TableDescriptor table,
    ColumnFamilyDescriptor family) {
if(KnobRuntime.check(java.util.UUID.fromString("3f8824b2-6405-34ee-b84a-8eb9beb35701"))) {
return null;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("f4b2e5c1-b0f4-30f4-a683-12081aa8eb55"))) ? (mergeConfigurations(global, table, family)) : (StoreUtils.createStoreConfiguration(global, table, family)));
  }

  static Class<? extends StoreFileTrackerBase>
    getStoreFileTrackerClassForMigration(Configuration conf, String configName) {
    String trackerName =
      Preconditions.checkNotNull(conf.get(configName), "config %s is not set", configName);
    try {
      return Trackers.valueOf(trackerName.toUpperCase()).clazz
        .asSubclass(StoreFileTrackerBase.class);
    } catch (IllegalArgumentException e) {
      // Fall back to them specifying a class name
      try {
        return Class.forName(trackerName).asSubclass(StoreFileTrackerBase.class);
      } catch (ClassNotFoundException cnfe) {
        throw new RuntimeException(cnfe);
      }
    }
  }

  /**
   * Create store file tracker to be used as source or destination for
   * {@link MigrationStoreFileTracker}.
   */
  static StoreFileTrackerBase createForMigration(Configuration conf, String configName,
    boolean isPrimaryReplica, StoreContext ctx) {
    Class<? extends StoreFileTrackerBase> tracker =
      getStoreFileTrackerClassForMigration(conf, configName);
    // prevent nest of MigrationStoreFileTracker, it will cause infinite recursion.
    if (MigrationStoreFileTracker.class.isAssignableFrom(tracker)) {
      throw new IllegalArgumentException("Should not specify " + configName + " as "
        + Trackers.MIGRATION + " because it can not be nested");
    }
    LOG.debug("instantiating StoreFileTracker impl {} as {}", tracker.getName(), configName);
    return ReflectionUtils.newInstance(tracker, conf, isPrimaryReplica, ctx);
  }

  public static TableDescriptor updateWithTrackerConfigs(Configuration conf,
    TableDescriptor descriptor) {
    // CreateTableProcedure needs to instantiate the configured SFT impl, in order to update table
    // descriptors with the SFT impl specific configs. By the time this happens, the table has no
    // regions nor stores yet, so it can't create a proper StoreContext.
    if (StringUtils.isEmpty(descriptor.getValue(TRACKER_IMPL))) {
      StoreFileTracker tracker = StoreFileTrackerFactory.create(conf, true, null);
      TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(descriptor);
      return tracker.updateWithTrackerConfigs(builder).build();
    }
    return descriptor;
  }

  public static boolean isMigration(Class<?> clazz) {
if(KnobRuntime.check(java.util.UUID.fromString("dd089960-6d53-346b-87ab-a0c3c8d41844"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("083c9918-2f7a-309e-82d0-95b31b4d2b4a"))) {
return true;
}
    return MigrationStoreFileTracker.class.isAssignableFrom(clazz);
  }
}
