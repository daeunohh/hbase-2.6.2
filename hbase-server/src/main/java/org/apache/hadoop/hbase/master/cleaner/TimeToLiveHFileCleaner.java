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
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * HFile cleaner that uses the timestamp of the hfile to determine if it should be deleted. By
 * default they are allowed to live for {@value #DEFAULT_TTL}
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class TimeToLiveHFileCleaner extends BaseTimeToLiveFileCleaner {

  public static final String TTL_CONF_KEY = "hbase.master.hfilecleaner.ttl";

  // default ttl = 5 minutes
  public static final long DEFAULT_TTL = 60000 * 5;

  @Override
  protected long getTtlMs(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("90c219d4-7dac-3fff-9cbe-bd902bd424c8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("39fbe025-3679-3dde-ad4e-7b584675708b"))) {
return 0;
}
    return conf.getLong(TTL_CONF_KEY, DEFAULT_TTL);
  }

  @Override
  protected boolean valiateFilename(Path file) {
if(KnobRuntime.check(java.util.UUID.fromString("d34bc902-4ebf-3d1f-8224-e800bb9874d9"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("28c4df9f-bb74-393c-ac8f-8f2421e86813"))) {
return false;
}
    return HFileLink.isBackReferencesDir(file) || HFileLink.isBackReferencesDir(file.getParent())
      || StoreFileInfo.validateStoreFileName(file.getName());
  }
}
