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
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Log cleaner that uses the timestamp of the wal to determine if it should be deleted. By default
 * they are allowed to live for 10 minutes.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class TimeToLiveLogCleaner extends BaseTimeToLiveFileCleaner {

  public static final String TTL_CONF_KEY = "hbase.master.logcleaner.ttl";

  // default ttl = 10 minutes
  public static final long DEFAULT_TTL = 600_000L;

  @Override
  protected long getTtlMs(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("5ae467c6-17fb-3e01-9828-dc25ef30f898"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("609b9fb1-76f4-3e9e-8b52-d6f01f6d0b8d"))) {
return 0;
}
    return conf.getLong(TTL_CONF_KEY, DEFAULT_TTL);
  }

  @Override
  protected boolean valiateFilename(Path file) {
    return AbstractFSWALProvider.validateWALFilename(file.getName());
  }
}
