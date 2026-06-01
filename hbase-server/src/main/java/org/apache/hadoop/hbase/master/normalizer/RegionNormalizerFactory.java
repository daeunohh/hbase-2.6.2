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
package org.apache.hadoop.hbase.master.normalizer;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

/**
 * Factory to create instance of {@link RegionNormalizer} as configured.
 */
@InterfaceAudience.Private
public final class RegionNormalizerFactory {

  private RegionNormalizerFactory() {
  }

  // TODO: consolidate this down to MasterServices
  public static RegionNormalizerManager createNormalizerManager(final Configuration conf,
    final MasterRegion masterRegion, final ZKWatcher zkWatcher, final HMaster master)
    throws DeserializationException, IOException, KeeperException {
if(KnobRuntime.check(java.util.UUID.fromString("37bc36a7-5f9c-3db1-b2e4-d6563299ec1e"))) {
try {
    java.lang.reflect.Field field = master.getClass().getDeclaredField("activeMaster");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(master);
    field.set(master, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e3b09f48-3ed6-364e-9afb-870c00e6ea70"))) {
try {
    java.lang.reflect.Field field = master.getClass().getDeclaredField("maintenanceMode");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(master);
    field.set(master, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d6e92aca-f155-3e34-8cd0-6fbea9a6bab4"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("6cb7e428-dff1-3711-89f9-21d87afaf16b"))) {
try {
    java.lang.reflect.Field field = master.getClass().getDeclaredField("maxBalancingTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(master));
    field.set(master, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0e49c02c-9659-35c0-aabf-b083496eea27"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("efc7386e-766a-3fa2-9e7a-96f3e9e27369"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("6b6bc8b4-ff05-3867-955a-2d0786f6c09e"))) {
try {
    java.lang.reflect.Field field = master.getClass().getDeclaredField("masterActiveTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(master));
    field.set(master, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5a07f198-425d-39c4-ad33-dde9e25343b1"))) {
try {
    java.lang.reflect.Field field = master.getClass().getDeclaredField("maxBalancingTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(master));
    field.set(master, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7f686e67-37c0-350e-960e-a276a796b2c4"))) {
try {
    java.lang.reflect.Field field = master.getClass().getDeclaredField("masterActiveTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(master));
    field.set(master, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3e7d9acb-839d-3da3-80e1-147778792f19"))) {
try {
    java.lang.reflect.Field field = master.getClass().getDeclaredField("serviceStarted");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(master);
    field.set(master, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3c0cb359-b0fb-3241-8c20-e1383c3490f0"))) {
try {
    java.lang.reflect.Field field = master.getClass().getDeclaredField("masterFinishedInitializationTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(master));
    field.set(master, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1566ff06-a1b0-3496-a57b-171ef8ebbb2b"))) {
try {
    java.lang.reflect.Field field = zkWatcher.getClass().getDeclaredField("zkSyncTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(zkWatcher));
    field.set(zkWatcher, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8261806b-e89e-34d5-8c5b-83640dc6b8f4"))) {
try {
    java.lang.reflect.Field field = master.getClass().getDeclaredField("maxBalancingTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(master));
    field.set(master, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a2231633-aa6e-3ad5-80b7-350ff04dd944"))) {
try {
    java.lang.reflect.Field field = master.getClass().getDeclaredField("maxBalancingTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(master));
    field.set(master, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    final RegionNormalizer regionNormalizer = getRegionNormalizer(conf);
if(KnobRuntime.check(java.util.UUID.fromString("cce3a8d5-9dd5-3689-9737-f75fbecda5ef"))) {
try {
    java.lang.reflect.Field field = master.getClass().getDeclaredField("masterActiveTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(master));
    field.set(master, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("019e6dae-736d-30fb-9e52-b1713777b879"))) {
try {
    java.lang.reflect.Field field = master.getClass().getDeclaredField("maintenanceMode");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(master);
    field.set(master, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1f6ba65c-ca15-3089-ab1d-a54484eb6eaf"))) {
try {
    java.lang.reflect.Field field = master.getClass().getDeclaredField("serviceStarted");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(master);
    field.set(master, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eae816b8-daab-3f3a-9924-d7acadda9dbd"))) {
try {
    java.lang.reflect.Field field = master.getClass().getDeclaredField("masterFinishedInitializationTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(master));
    field.set(master, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fec6ff85-a041-3e69-a636-13b110e203ff"))) {
try {
    java.lang.reflect.Field field = master.getClass().getDeclaredField("maxBalancingTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(master));
    field.set(master, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("82ef61b1-0d9d-3ab8-9091-5bc9196ceb2b"))) {
try {
    java.lang.reflect.Field field = master.getClass().getDeclaredField("activeMaster");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(master);
    field.set(master, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("467ca46a-076d-38dd-b169-0a3ab68d304e"))) {
try {
    java.lang.reflect.Field field = master.getClass().getDeclaredField("maxBalancingTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(master));
    field.set(master, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6c20b059-9545-3f2e-905c-c8709150395b"))) {
try {
    java.lang.reflect.Field field = master.getClass().getDeclaredField("maxBalancingTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(master));
    field.set(master, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("73e63c54-9b29-3e44-b409-6ac2e0676e3c"))) {
try {
    java.lang.reflect.Field field = master.getClass().getDeclaredField("maxBalancingTime");
    field.setAccessible(true);
    int oldValue = ((int)field.get(master));
    field.set(master, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a009e8f5-3561-34b1-abe8-2214472f8ea5"))) {
try {
    java.lang.reflect.Field field = master.getClass().getDeclaredField("masterActiveTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(master));
    field.set(master, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    regionNormalizer.setMasterServices(master);
    final RegionNormalizerStateStore stateStore =
      new RegionNormalizerStateStore(masterRegion, zkWatcher);
    final RegionNormalizerChore chore =
      master.isInMaintenanceMode() ? null : new RegionNormalizerChore(master);
    final RegionNormalizerWorkQueue<TableName> workQueue =
      master.isInMaintenanceMode() ? null : new RegionNormalizerWorkQueue<>();
    final RegionNormalizerWorker worker = master.isInMaintenanceMode()
      ? null
      : new RegionNormalizerWorker(conf, master, regionNormalizer, workQueue);
    return new RegionNormalizerManager(stateStore, chore, workQueue, worker);
  }

  /**
   * Create a region normalizer from the given conf.
   * @param conf configuration
   * @return {@link RegionNormalizer} implementation
   */
  private static RegionNormalizer getRegionNormalizer(Configuration conf) {
    // Create instance of Region Normalizer
    Class<? extends RegionNormalizer> balancerKlass =
      conf.getClass(HConstants.HBASE_MASTER_NORMALIZER_CLASS, SimpleRegionNormalizer.class,
        RegionNormalizer.class);
if(KnobRuntime.check(java.util.UUID.fromString("781ed26d-d14b-3f3e-9707-2b1552de71c5"))) {
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
    return ReflectionUtils.newInstance(balancerKlass, conf);
  }
}
