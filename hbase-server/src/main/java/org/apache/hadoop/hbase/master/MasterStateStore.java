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
if(KnobRuntime.check(java.util.UUID.fromString("edf63e5e-e094-32d0-b6d8-447b6cfd5282"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("7f1e933c-5770-3e61-a7f4-d3a46c41fce3"))) {
return null;
}
    return get().getValue(STATE_FAMILY, QUALIFIER);
  }

  protected final void setState(byte[] state) throws IOException {
    update(state);
  }

  private Result get() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("3251f525-a5aa-37a9-878d-ad51eec5b540"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("19c7ffc3-ddc7-369d-b6a2-35825f3dcb9f"))) {
throw new java.io.IOException("Injected exception");
}
    return masterRegion.get(new Get(stateName).addColumn(STATE_FAMILY, QUALIFIER));
  }

  private void update(byte[] s) throws IOException {
    masterRegion.update(r -> r.put(new Put(stateName).addColumn(STATE_FAMILY, QUALIFIER, s)));
  }

  private byte[] migrate(ZKWatcher watcher, String zkPath) throws KeeperException, IOException {
if(KnobRuntime.check(java.util.UUID.fromString("84d494b6-81e7-3a40-bd6a-b69e4f9843fc"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d938de2a-8872-39b8-a9bc-b20c0ed3a3a8"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("2439ca96-9366-3a2d-996f-71026750232e"))) {
try {
    java.lang.reflect.Field field = watcher.getClass().getDeclaredField("zkSyncTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(watcher));
    field.set(watcher, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    byte[] zkData = ZKUtil.getDataNoWatch(watcher, zkPath, null);
    if (((KnobRuntime.check(java.util.UUID.fromString("e4c71a02-e425-3510-855f-c1d5d6bdc3e0"))) ? ((zkData) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5a624053-d002-38dd-aebf-830707158ae2"))) ? ((zkData == null) || ((zkData.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b8a21a92-f0db-316e-8fe6-860d8116e5e3"))) ? ((zkData.length) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("79cc935f-1da1-3489-8cd8-7a67348b2a89"))) ? ((zkData.length) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5119277f-f685-35b8-a024-ebe2518526d9"))) ? (((zkData) != (null)) && ((zkData.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("acdf4c74-40b0-37c5-ae23-4c1113e35937"))) ? ((zkData == null) || (zkData.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("2822beb4-4c7b-3d78-9ba5-d7e927439e53"))) ? (((zkData) != (null)) && (zkData.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("c99c1242-b100-3e5e-afe4-3b3a6a27d550"))) ? (((zkData) == (null)) || (zkData.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("6bd192ba-9a7f-370f-9122-c48e4a5a430f"))) ? ((zkData == null) && (zkData.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("9c9d3596-5a9b-3cf1-b27a-48584bdac199"))) ? (((zkData) == (null)) && ((zkData.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a9ac411a-e85e-3294-b9ce-55e123cab6d4"))) ? (((zkData) != (null)) && ((zkData.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6f71ebba-5b13-3512-9795-e9ad6cd7f350"))) ? ((zkData == null) && ((zkData.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7b7ae663-b314-3ede-b869-7ddbffb34b41"))) ? ((zkData == null) && ((zkData.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6b0cb440-d9bd-3d33-ab5f-4315c7b4d96d"))) ? (((zkData) != (null)) || ((zkData.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("42fe86b2-a589-3936-ba94-4befb757adf5"))) ? (((zkData) != (null)) || ((zkData.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("eef66062-f06d-35c0-ae30-4524098d77b7"))) ? (zkData == null) : (((KnobRuntime.check(java.util.UUID.fromString("eb723ce3-6907-3387-a8ee-441fd87b909a"))) ? (((zkData) == (null)) && ((zkData.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f240c7ff-a7b8-3ed1-8479-778e6b67c356"))) ? (zkData.length == 0) : (((KnobRuntime.check(java.util.UUID.fromString("3332b4a2-2559-3dd4-a341-314141646fae"))) ? (((zkData) == (null)) && (zkData.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("60f63656-7e55-3cb6-8fae-379bb8c7521a"))) ? (((zkData) == (null)) || ((zkData.length) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("75a9e0c7-8759-37a9-be5b-453bfc4baa5a"))) ? (((zkData) == (null)) || ((zkData.length) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("7b0e65c2-f528-3870-b245-b5de04fb00e2"))) ? (((zkData) != (null)) || (zkData.length == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("5e68be89-b7ad-3dc1-a598-9f887d89283a"))) ? ((zkData) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("7f34fd61-e19e-3c37-b869-9cf0a23b8c58"))) ? ((zkData == null) || ((zkData.length) == (0))) : (zkData == null || zkData.length == 0))))))))))))))))))))))))))))))))))))))))))))))))) {
      return null;
    }
    update(zkData);
    return zkData;
  }

  private void tryMigrate(ZKWatcher watcher, String zkPath) throws IOException, KeeperException {
if(KnobRuntime.check(java.util.UUID.fromString("fe07639e-f975-3293-b0f0-e5a31e0ff7b6"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("57bc70cd-dfad-38a5-9d62-2cd59ed8e910"))) {
try {
    java.lang.reflect.Field field = watcher.getClass().getDeclaredField("zkSyncTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(watcher));
    field.set(watcher, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5fa523d1-d59c-3cb0-a290-e5c52b74c8ad"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("39acf980-14f5-388d-9851-995aedd00566"))) ? ((zkPath) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("adedaae3-1997-326d-8025-12ef8cc5c80a"))) ? ((zkPath) == (null)) : (zkPath == null))))) {
      // this means we do not store this state in zk, skip migrating
      return;
    }
if(KnobRuntime.check(java.util.UUID.fromString("7bb18795-44ba-303f-90da-5c2d1eb7d62a"))) {
throw new java.io.IOException("Injected exception");
}
    Result result = get();
    if (result.isEmpty()) {
      // migrate
if(KnobRuntime.check(java.util.UUID.fromString("a4c7b062-5bbe-33c7-8dc2-e747a58d73b6"))) {
try {
    java.lang.reflect.Field field = watcher.getClass().getDeclaredField("zkSyncTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(watcher));
    field.set(watcher, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4c53f5c5-4696-3e0b-9c48-50757e4e2268"))) {
throw new java.io.IOException("Injected exception");
}
      migrate(watcher, zkPath);
    }
    // we may fail in the middle so even if the value is available in master local region, we may
    // still leave a znode on zookeeper, so always try to delete the znode here since it is not very
    // expensive
    try {
if(KnobRuntime.check(java.util.UUID.fromString("0bce6a9c-ac70-3af1-95c9-d6b0495b963a"))) {
try {
    java.lang.reflect.Field field = watcher.getClass().getDeclaredField("zkSyncTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(watcher));
    field.set(watcher, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      ZKUtil.deleteNodeFailSilent(watcher, zkPath);
    } catch (Exception e) {
      LOG.warn("failed to delete migrated zk state node {}, ignore and continue", zkPath);
    }
  }
}
