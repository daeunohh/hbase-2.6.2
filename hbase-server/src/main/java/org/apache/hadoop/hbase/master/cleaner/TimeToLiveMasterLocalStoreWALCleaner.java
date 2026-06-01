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
 * Master local storage WAL cleaner that uses the timestamp of the WAL file to determine if it
 * should be deleted. By default they are allowed to live for {@value #DEFAULT_TTL}
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class TimeToLiveMasterLocalStoreWALCleaner extends BaseTimeToLiveFileCleaner {

  public static final String TTL_CONF_KEY = "hbase.master.local.store.walcleaner.ttl";

  // default ttl = 7 days
  public static final long DEFAULT_TTL = 604_800_000L;

  @Override
  protected long getTtlMs(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("95829e9b-6bdb-3d4c-a22c-38e2eeca2db3"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("921654d6-1c48-3a30-8c4f-024841bbe5c7"))) {
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
    return file.getName().endsWith(MasterRegionFactory.ARCHIVED_WAL_SUFFIX);
  }
}
