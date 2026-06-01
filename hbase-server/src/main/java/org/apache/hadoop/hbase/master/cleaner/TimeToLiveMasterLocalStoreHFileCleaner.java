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
package org.apache.hadoop.hbase.master.cleaner;
import org.knobinjection.runtime.KnobRuntime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Master local storage HFile cleaner that uses the timestamp of the HFile to determine if it should
 * be deleted. By default they are allowed to live for {@value #DEFAULT_TTL}
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class TimeToLiveMasterLocalStoreHFileCleaner extends BaseTimeToLiveFileCleaner {

  public static final String TTL_CONF_KEY = "hbase.master.local.store.hfilecleaner.ttl";

  // default ttl = 7 days
  public static final long DEFAULT_TTL = 604_800_000L;

  @Override
  protected long getTtlMs(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("1ff25940-1257-38e5-bd93-00ef5166cf1c"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("13c0d791-6e73-335b-8bea-376c623166e8"))) {
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
    return conf.getLong(TTL_CONF_KEY, DEFAULT_TTL);
  }

  @Override
  protected boolean valiateFilename(Path file) {
if(KnobRuntime.check(java.util.UUID.fromString("8bbefbe8-0f15-3cf7-a0ec-fb7e673f2d26"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("abc317b7-a678-3b9b-8ed4-a589faf4b2b3"))) {
return false;
}
    return file.getName().endsWith(MasterRegionFactory.ARCHIVED_HFILE_SUFFIX);
  }

}
