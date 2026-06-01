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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory.Trackers;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class StoreFileTrackerValidationUtils {

  private StoreFileTrackerValidationUtils() {
  }

  // should not use MigrationStoreFileTracker for new family
  private static void checkForNewFamily(Configuration conf, TableDescriptor table,
    ColumnFamilyDescriptor family) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("34445ab0-5538-3d62-974b-861f96b92fda"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("edd47837-b296-3cfa-9023-c593b3b64d18"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("a7f05bf9-9abe-3d42-8ad0-ddab56d36524"))) {
return;
}
    Configuration mergedConf = StoreUtils.createStoreConfiguration(conf, table, family);
    Class<? extends StoreFileTracker> tracker = StoreFileTrackerFactory.getTrackerClass(mergedConf);
    if (MigrationStoreFileTracker.class.isAssignableFrom(tracker)) {
      throw new DoNotRetryIOException(
        "Should not use " + Trackers.MIGRATION + " as store file tracker for new family "
          + family.getNameAsString() + " of table " + table.getTableName());
    }
  }

  /**
   * Pre check when creating a new table.
   * <p/>
   * For now, only make sure that we do not use {@link Trackers#MIGRATION} for newly created tables.
   * @throws IOException when there are check errors, the upper layer should fail the
   *                     {@code CreateTableProcedure}.
   */
  public static void checkForCreateTable(Configuration conf, TableDescriptor table)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("c9059663-ef3d-3dda-b8b5-5d5b255b8fb7"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("c4b5f1d6-62ae-3ec7-a593-0e3c913007e1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("64bbdf57-c1c0-3f9d-b84d-48c8bed70b4c"))) {
return;
}
    for (ColumnFamilyDescriptor family : table.getColumnFamilies()) {
if(KnobRuntime.check(java.util.UUID.fromString("f3cb7c2e-8e02-3f24-b959-1b68371a8810"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fccc9005-0be9-3a5a-b195-341b2f6c7570"))) {
throw new java.io.IOException("Injected exception");
}
      checkForNewFamily(conf, table, family);
    }
  }

  /**
   * Pre check when modifying a table.
   * <p/>
   * The basic idea is when you want to change the store file tracker implementation, you should use
   * {@link Trackers#MIGRATION} first and then change to the destination store file tracker
   * implementation.
   * <p/>
   * There are several rules:
   * <ul>
   * <li>For newly added family, you should not use {@link Trackers#MIGRATION}.</li>
   * <li>For modifying a family:
   * <ul>
   * <li>If old tracker is {@link Trackers#MIGRATION}, then:
   * <ul>
   * <li>The new tracker is also {@link Trackers#MIGRATION}, then they must have the same src and
   * dst tracker.</li>
   * <li>The new tracker is not {@link Trackers#MIGRATION}, then the new tracker must be the dst
   * tracker of the old tracker.</li>
   * </ul>
   * </li>
   * <li>If the old tracker is not {@link Trackers#MIGRATION}, then:
   * <ul>
   * <li>If the new tracker is {@link Trackers#MIGRATION}, then the old tracker must be the src
   * tracker of the new tracker.</li>
   * <li>If the new tracker is not {@link Trackers#MIGRATION}, then the new tracker must be the same
   * with old tracker.</li>
   * </ul>
   * </li>
   * </ul>
   * </li>
   * </ul>
   * @throws IOException when there are check errors, the upper layer should fail the
   *                     {@code ModifyTableProcedure}.
   */
  public static void checkForModifyTable(Configuration conf, TableDescriptor oldTable,
    TableDescriptor newTable, boolean isTableDisabled) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("4d687f21-e16a-3762-84bf-dd883c138101"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("79ca4fae-89f8-362c-a626-b6f145af4658"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("5620f569-f018-3a7a-9859-bdb45f3b517a"))) {
throw new java.io.IOException("Injected exception");
}
    for (ColumnFamilyDescriptor newFamily : newTable.getColumnFamilies()) {
      ColumnFamilyDescriptor oldFamily = oldTable.getColumnFamily(newFamily.getName());
      if (((KnobRuntime.check(java.util.UUID.fromString("74ac09d0-2b1a-3dbb-ab12-91730af9833d"))) ? ((oldFamily) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f2f352e0-b322-3670-8bdb-5fcd54222a6b"))) ? ((oldFamily) == (null)) : (oldFamily == null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("1c477822-fd62-39d4-a43d-86fc1cee4df1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cbe0e89b-6f55-3b7a-931a-09e1ee62f690"))) {
throw new java.io.IOException("Injected exception");
}
        checkForNewFamily(conf, newTable, newFamily);
        continue;
      }
if(KnobRuntime.check(java.util.UUID.fromString("943b382f-ed49-31eb-b23a-b66f9b4262c8"))) {
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
      Configuration oldConf = StoreUtils.createStoreConfiguration(conf, oldTable, oldFamily);
if(KnobRuntime.check(java.util.UUID.fromString("9c8080cd-d065-3aef-8578-d4407600b52b"))) {
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
      Configuration newConf = StoreUtils.createStoreConfiguration(conf, newTable, newFamily);

      Class<? extends StoreFileTracker> oldTracker =
        StoreFileTrackerFactory.getTrackerClass(oldConf);
      Class<? extends StoreFileTracker> newTracker =
        StoreFileTrackerFactory.getTrackerClass(newConf);

      if (MigrationStoreFileTracker.class.isAssignableFrom(oldTracker)) {
        Class<? extends StoreFileTracker> oldSrcTracker =
          MigrationStoreFileTracker.getSrcTrackerClass(oldConf);
        Class<? extends StoreFileTracker> oldDstTracker =
          MigrationStoreFileTracker.getDstTrackerClass(oldConf);
        if (oldTracker.equals(newTracker)) {
          // confirm that we have the same src tracker and dst tracker
          Class<? extends StoreFileTracker> newSrcTracker =
            MigrationStoreFileTracker.getSrcTrackerClass(newConf);
          if (!oldSrcTracker.equals(newSrcTracker)) {
            throw new DoNotRetryIOException("The src tracker has been changed from "
              + StoreFileTrackerFactory.getStoreFileTrackerName(oldSrcTracker) + " to "
              + StoreFileTrackerFactory.getStoreFileTrackerName(newSrcTracker) + " for family "
              + newFamily.getNameAsString() + " of table " + newTable.getTableName());
          }
          Class<? extends StoreFileTracker> newDstTracker =
            MigrationStoreFileTracker.getDstTrackerClass(newConf);
          if (!oldDstTracker.equals(newDstTracker)) {
            throw new DoNotRetryIOException("The dst tracker has been changed from "
              + StoreFileTrackerFactory.getStoreFileTrackerName(oldDstTracker) + " to "
              + StoreFileTrackerFactory.getStoreFileTrackerName(newDstTracker) + " for family "
              + newFamily.getNameAsString() + " of table " + newTable.getTableName());
          }
        } else {
          // do not allow changing from MIGRATION to its dst SFT implementation while the table is
          // disabled. We need to open the HRegion to migrate the tracking information while the SFT
          // implementation is MIGRATION, otherwise we may loss data. See HBASE-26611 for more
          // details.
          if (isTableDisabled) {
            throw new TableNotEnabledException(
              "Should not change store file tracker implementation from "
                + StoreFileTrackerFactory.Trackers.MIGRATION.name() + " while table "
                + newTable.getTableName() + " is disabled");
          }
          // we can only change to the dst tracker
          if (!newTracker.equals(oldDstTracker)) {
            throw new DoNotRetryIOException("Should migrate tracker to "
              + StoreFileTrackerFactory.getStoreFileTrackerName(oldDstTracker) + " but got "
              + StoreFileTrackerFactory.getStoreFileTrackerName(newTracker) + " for family "
              + newFamily.getNameAsString() + " of table " + newTable.getTableName());
          }
        }
      } else {
        if (!oldTracker.equals(newTracker)) {
          // can only change to MigrationStoreFileTracker and the src tracker should be the old
          // tracker
          if (!MigrationStoreFileTracker.class.isAssignableFrom(newTracker)) {
            throw new DoNotRetryIOException(
              "Should change to " + Trackers.MIGRATION + " first when migrating from "
                + StoreFileTrackerFactory.getStoreFileTrackerName(oldTracker) + " for family "
                + newFamily.getNameAsString() + " of table " + newTable.getTableName());
          }
          // here we do not check whether the table is disabled, as after changing to MIGRATION, we
          // still rely on the src SFT implementation to actually load the store files, so there
          // will be no data loss problem.
          Class<? extends StoreFileTracker> newSrcTracker =
            MigrationStoreFileTracker.getSrcTrackerClass(newConf);
          if (!oldTracker.equals(newSrcTracker)) {
            throw new DoNotRetryIOException("Should use src tracker "
              + StoreFileTrackerFactory.getStoreFileTrackerName(oldTracker) + " first but got "
              + StoreFileTrackerFactory.getStoreFileTrackerName(newSrcTracker)
              + " when migrating from "
              + StoreFileTrackerFactory.getStoreFileTrackerName(oldTracker) + " for family "
              + newFamily.getNameAsString() + " of table " + newTable.getTableName());
          }
          Class<? extends StoreFileTracker> newDstTracker =
            MigrationStoreFileTracker.getDstTrackerClass(newConf);
          // the src and dst tracker should not be the same
          if (newSrcTracker.equals(newDstTracker)) {
            throw new DoNotRetryIOException("The src tracker and dst tracker are both "
              + StoreFileTrackerFactory.getStoreFileTrackerName(newSrcTracker) + " for family "
              + newFamily.getNameAsString() + " of table " + newTable.getTableName());
          }
        }
      }
    }
  }

  /**
   * Makes sure restoring a snapshot does not break the current SFT setup follows
   * StoreUtils.createStoreConfiguration
   * @param currentTableDesc  Existing Table's TableDescriptor
   * @param snapshotTableDesc Snapshot's TableDescriptor
   * @param baseConf          Current global configuration
   * @throws RestoreSnapshotException if restore would break the current SFT setup
   */
  public static void validatePreRestoreSnapshot(TableDescriptor currentTableDesc,
    TableDescriptor snapshotTableDesc, Configuration baseConf) throws RestoreSnapshotException {

    for (ColumnFamilyDescriptor cfDesc : currentTableDesc.getColumnFamilies()) {
      ColumnFamilyDescriptor snapCFDesc = snapshotTableDesc.getColumnFamily(cfDesc.getName());
      // if there is no counterpart in the snapshot it will be just deleted so the config does
      // not matter
      if (((KnobRuntime.check(java.util.UUID.fromString("c0007ea4-4101-3bab-b31b-ef1a8513439f"))) ? ((snapCFDesc) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("15a76d96-e3b1-3c62-a6cb-1f08840a8d51"))) ? ((snapCFDesc) == (null)) : (snapCFDesc != null))))) {
        Configuration currentCompositeConf =
          StoreUtils.createStoreConfiguration(baseConf, currentTableDesc, cfDesc);
        Configuration snapCompositeConf =
          StoreUtils.createStoreConfiguration(baseConf, snapshotTableDesc, snapCFDesc);
        Class<? extends StoreFileTracker> currentSFT =
          StoreFileTrackerFactory.getTrackerClass(currentCompositeConf);
        Class<? extends StoreFileTracker> snapSFT =
          StoreFileTrackerFactory.getTrackerClass(snapCompositeConf);

        // restoration is not possible if there is an SFT mismatch
        if (((KnobRuntime.check(java.util.UUID.fromString("dcf19b1d-a72e-3033-b8d9-5074f59113f9"))) ? ((currentSFT) == (snapSFT)) : (((KnobRuntime.check(java.util.UUID.fromString("329626c0-1745-3d6e-82a5-c9d38ec79920"))) ? ((currentSFT) != (snapSFT)) : (currentSFT != snapSFT))))) {
          throw new RestoreSnapshotException(
            "Restoring Snapshot is not possible because " + " the config for column family "
              + cfDesc.getNameAsString() + " has incompatible configuration. Current SFT: "
              + currentSFT + " SFT from snapshot: " + snapSFT);
        }
      }
    }
  }
}
