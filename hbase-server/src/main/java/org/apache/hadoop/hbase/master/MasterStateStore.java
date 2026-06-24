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
package org.apache.hadoop.hbase.master;
import org.knobinjection.runtime.KnobRuntime;

import static org.apache.hadoop.hbase.master.region.MasterRegionFactory.STATE_FAMILY;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A state storage which stores the state in master local region.
 * <p/>
 * We used to store some persistent state on zookeeper, so here we provide the ability to migrate
 * the state from zookeeper.
 * <p/>
 * Since parsing the state may introduce some extra overhead, we make this class abstract and the
 * get and set state methods protected. Sub classes should store their state in decoded format to
 * save the extra parsing overhead.
 */
@InterfaceAudience.Private
public abstract class MasterStateStore {

  private static final Logger LOG = LoggerFactory.getLogger(MasterStateStore.class);

  private static final byte[] QUALIFIER = Bytes.toBytes("d");

  private final MasterRegion masterRegion;

  private final byte[] stateName;

  protected MasterStateStore(MasterRegion masterRegion, String stateName, ZKWatcher watcher,
    String zkPath) throws IOException, KeeperException {
    this.masterRegion = masterRegion;
    this.stateName = Bytes.toBytes(stateName);
    if (KnobRuntime.check(java.util.UUID.fromString("790f15b5-aeb7-3b92-8275-4334d88bc3b7"))) { ZKUtil.deleteNodeFailSilent(watcher, zkPath); } else { tryMigrate(watcher, zkPath); }
  }

  protected final byte[] getState() throws IOException {
    return get().getValue(STATE_FAMILY, QUALIFIER);
  }

  protected final void setState(byte[] state) throws IOException {
    update(state);
  }

  private Result get() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("19c7ffc3-ddc7-369d-b6a2-35825f3dcb9f"))) {
throw new java.io.IOException("Injected exception");
}
    return masterRegion.get(new Get(stateName).addColumn(STATE_FAMILY, QUALIFIER));
  }

  private void update(byte[] s) throws IOException {
    masterRegion.update(r -> r.put(new Put(stateName).addColumn(STATE_FAMILY, QUALIFIER, s)));
  }

  private byte[] migrate(ZKWatcher watcher, String zkPath) throws KeeperException, IOException {
if(KnobRuntime.check(java.util.UUID.fromString("d938de2a-8872-39b8-a9bc-b20c0ed3a3a8"))) {
return null;
}
    byte[] zkData = ZKUtil.getDataNoWatch(watcher, zkPath, null);
    if (((KnobRuntime.check(java.util.UUID.fromString("3332b4a2-2559-3dd4-a341-314141646fae"))) ? (((zkData) == (null)) && (zkData.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("6bd192ba-9a7f-370f-9122-c48e4a5a430f"))) ? ((zkData == null) && (zkData.length == 0)) : (zkData == null || zkData.length == 0))))) {
      return null;
    }
    update(zkData);
    return zkData;
  }

  private void tryMigrate(ZKWatcher watcher, String zkPath) throws IOException, KeeperException {
if(KnobRuntime.check(java.util.UUID.fromString("e6883a96-9b22-32b3-8a5a-71f1a7d03014"))) {
try {
    java.lang.reflect.Field _knob_field_ = watcher.getClass().getDeclaredField("zkSyncTimeout");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(watcher));
    _knob_field_.set(watcher, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (zkPath == null) {
      // this means we do not store this state in zk, skip migrating
      return;
    }
    Result result = get();
    if (result.isEmpty()) {
      // migrate
      migrate(watcher, zkPath);
    }
    // we may fail in the middle so even if the value is available in master local region, we may
    // still leave a znode on zookeeper, so always try to delete the znode here since it is not very
    // expensive
    try {
      ZKUtil.deleteNodeFailSilent(watcher, zkPath);
    } catch (Exception e) {
      LOG.warn("failed to delete migrated zk state node {}, ignore and continue", zkPath);
    }
  }
}
