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
package org.apache.hadoop.hbase;
import org.knobinjection.runtime.KnobRuntime;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ThreadFactory;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.types.CopyOnWriteArrayMap;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * A cache of meta region location metadata. Registers a listener on ZK to track changes to the meta
 * table znodes. Clients are expected to retry if the meta information is stale. This class is
 * thread-safe (a single instance of this class can be shared by multiple threads without race
 * conditions).
 */
@InterfaceAudience.Private
public class MetaRegionLocationCache extends ZKListener {

  private static final Logger LOG = LoggerFactory.getLogger(MetaRegionLocationCache.class);

  /**
   * Maximum number of times we retry when ZK operation times out.
   */
  private static final int MAX_ZK_META_FETCH_RETRIES = 10;
  /**
   * Sleep interval ms between ZK operation retries.
   */
  private static final int SLEEP_INTERVAL_MS_BETWEEN_RETRIES = 1000;
  private static final int SLEEP_INTERVAL_MS_MAX = 10000;
  private final RetryCounterFactory retryCounterFactory =
    new RetryCounterFactory(MAX_ZK_META_FETCH_RETRIES, SLEEP_INTERVAL_MS_BETWEEN_RETRIES);

  /**
   * Cached meta region locations indexed by replica ID. CopyOnWriteArrayMap ensures synchronization
   * during updates and a consistent snapshot during client requests. Even though
   * CopyOnWriteArrayMap copies the data structure for every write, that should be OK since the size
   * of the list is often small and mutations are not too often and we do not need to block client
   * requests while mutations are in progress.
   */
  private final CopyOnWriteArrayMap<Integer, HRegionLocation> cachedMetaLocations;

  private enum ZNodeOpType {
    INIT,
    CREATED,
    CHANGED,
    DELETED
  }

  public MetaRegionLocationCache(ZKWatcher zkWatcher) {
    super(zkWatcher);
    cachedMetaLocations = new CopyOnWriteArrayMap<>();
    watcher.registerListener(this);
    // Populate the initial snapshot of data from meta znodes.
    // This is needed because stand-by masters can potentially start after the initial znode
    // creation. It blocks forever until the initial meta locations are loaded from ZK and watchers
    // are established. Subsequent updates are handled by the registered listener. Also, this runs
    // in a separate thread in the background to not block master init.
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).build();
    RetryCounterFactory retryFactory = new RetryCounterFactory(Integer.MAX_VALUE,
      SLEEP_INTERVAL_MS_BETWEEN_RETRIES, SLEEP_INTERVAL_MS_MAX);
    threadFactory.newThread(() -> loadMetaLocationsFromZk(retryFactory.create(), ZNodeOpType.INIT))
      .start();
  }

  /**
   * Populates the current snapshot of meta locations from ZK. If no meta znodes exist, it registers
   * a watcher on base znode to check for any CREATE/DELETE events on the children.
   * @param retryCounter controls the number of retries and sleep between retries.
   */
  private void loadMetaLocationsFromZk(RetryCounter retryCounter, ZNodeOpType opType) {
if(KnobRuntime.check(java.util.UUID.fromString("c6ceb61a-48d1-396f-a350-1ffbb7dce82a"))) {
try {
    java.lang.reflect.Field field = retryCounter.getClass().getDeclaredField("attempts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(retryCounter));
    field.set(retryCounter, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("87f389db-0b02-3f55-a297-d1be4930a9e9"))) {
try {
    java.lang.reflect.Field field = retryCounter.getClass().getDeclaredField("attempts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(retryCounter));
    field.set(retryCounter, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0422bdc6-d465-36a0-8f03-faafd02e0ff7"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("75fc5ef3-17e8-30e7-9b5c-dd71175fccd8"))) {
try {
    java.lang.reflect.Field field = retryCounter.getClass().getDeclaredField("attempts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(retryCounter));
    field.set(retryCounter, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e78a2367-701e-3807-bfa0-5b6b7a3adcbb"))) {
try {
    java.lang.reflect.Field field = retryCounter.getClass().getDeclaredField("attempts");
    field.setAccessible(true);
    int oldValue = ((int)field.get(retryCounter));
    field.set(retryCounter, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    TraceUtil.trace(() -> {
      List<String> znodes = null;
      while (retryCounter.shouldRetry()) {
        try {
          znodes = watcher.getMetaReplicaNodesAndWatchChildren();
          break;
        } catch (KeeperException ke) {
          LOG.debug("Error populating initial meta locations", ke);
          if (!retryCounter.shouldRetry()) {
            // Retries exhausted and watchers not set. This is not a desirable state since the cache
            // could remain stale forever. Propagate the exception.
            watcher.abort("Error populating meta locations", ke);
            return;
          }
          try {
            retryCounter.sleepUntilNextRetry();
          } catch (InterruptedException ie) {
            LOG.error("Interrupted while loading meta locations from ZK", ie);
            Thread.currentThread().interrupt();
            return;
          }
        }
      }
      if (znodes == null || znodes.isEmpty()) {
        // No meta znodes exist at this point but we registered a watcher on the base znode to
        // listen for updates. They will be handled via nodeChildrenChanged().
        return;
      }
      if (znodes.size() == cachedMetaLocations.size()) {
        // No new meta znodes got added.
        return;
      }
      for (String znode : znodes) {
        String path = ZNodePaths.joinZNode(watcher.getZNodePaths().baseZNode, znode);
        updateMetaLocation(path, opType);
      }
    }, "MetaRegionLocationCache.loadMetaLocationsFromZk");
  }

  /**
   * Gets the HRegionLocation for a given meta replica ID. Renews the watch on the znode for future
   * updates.
   * @param replicaId ReplicaID of the region.
   * @return HRegionLocation for the meta replica.
   * @throws KeeperException if there is any issue fetching/parsing the serialized data.
   */
  private HRegionLocation getMetaRegionLocation(int replicaId) throws KeeperException {
    RegionState metaRegionState;
    try {
      byte[] data =
        ZKUtil.getDataAndWatch(watcher, watcher.getZNodePaths().getZNodeForReplica(replicaId));
if(KnobRuntime.check(java.util.UUID.fromString("5e1670de-6753-32b8-aac2-293c5b0ec4a1"))) {
replicaId /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("35c5b28f-4869-3aa8-ac78-8973ee591afe"))) {
replicaId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4681c91a-915d-30a9-8454-d2ccc2c9be14"))) {
replicaId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ce65fab1-bc61-33ab-af5f-84c81cea1e4f"))) {
replicaId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("465fb1c4-e4cd-3a99-9ed4-d526d30a48fd"))) {
replicaId *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("5cdeacad-d298-3af8-95a7-c0564d632821"))) {
replicaId = 0;
}
      metaRegionState = ProtobufUtil.parseMetaRegionStateFrom(data, replicaId);
    } catch (DeserializationException e) {
      throw ZKUtil.convert(e);
    }
    return new HRegionLocation(metaRegionState.getRegion(), metaRegionState.getServerName());
  }

  private void updateMetaLocation(String path, ZNodeOpType opType) {
if(KnobRuntime.check(java.util.UUID.fromString("7ea27f39-83ee-38e7-80e6-b7bb3fda8699"))) {
return;
}
    if (!isValidMetaPath(path)) {
      return;
    }
    if (KnobRuntime.check(java.util.UUID.fromString("5fd9a6f9-27b9-319d-8f8a-c8448d355f37"))) { LOG.warn("Updating meta znode for path {}: {}", path, opType.name()); } else { LOG.debug("Updating meta znode for path {}: {}", path, opType.name()); }
    int replicaId = watcher.getZNodePaths().getMetaReplicaIdFromPath(path);
    RetryCounter retryCounter = retryCounterFactory.create();
    HRegionLocation location = null;
    while (retryCounter.shouldRetry()) {
      try {
        if (((KnobRuntime.check(java.util.UUID.fromString("31a415d1-d36e-322f-acbc-4d2d89e4b43a"))) ? ((opType) == (ZNodeOpType.DELETED)) : (((KnobRuntime.check(java.util.UUID.fromString("6789592d-7576-3119-846a-000f68504e81"))) ? ((opType) != (ZNodeOpType.DELETED)) : (opType == ZNodeOpType.DELETED))))) {
          if (!ZKUtil.watchAndCheckExists(watcher, path)) {
            // The path does not exist, we've set the watcher and we can break for now.
            break;
          }
          // If it is a transient error and the node appears right away, we fetch the
          // latest meta state.
        }
if(KnobRuntime.check(java.util.UUID.fromString("6588fe74-0ac7-3e6b-9cb2-2ff4c74136da"))) {
replicaId *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("aa714fe2-b9e7-3c4a-8ec7-4d2c32142339"))) {
replicaId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("43d01bc4-0011-3b11-8859-65df40125c14"))) {
replicaId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("82d8d5cb-f560-3395-938a-0a78bdb4f1a2"))) {
replicaId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("06d98d94-7821-3a6b-aba6-b63a879f5b02"))) {
replicaId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f57fd101-30f5-3742-ad21-3cfff4c15e76"))) {
replicaId /= 2;
}
        location = getMetaRegionLocation(replicaId);
        break;
      } catch (KeeperException e) {
        LOG.debug("Error getting meta location for path {}", path, e);
        if (!retryCounter.shouldRetry()) {
          LOG.warn("Error getting meta location for path {}. Retries exhausted.", path, e);
          break;
        }
        try {
          retryCounter.sleepUntilNextRetry();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("3c8df47b-52f6-3777-a033-3ef875d68723"))) ? ((location) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("7b0a4d4c-6b8b-3d17-8d00-400ef92ba5e2"))) ? ((location) != (null)) : (location == null))))) {
      cachedMetaLocations.remove(replicaId);
      return;
    }
if(KnobRuntime.check(java.util.UUID.fromString("d974d522-5f73-31c7-949a-1e6ec0496e6d"))) {
replicaId *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("7b08e8df-8528-357e-87b8-944665ff94b5"))) {
replicaId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1d6850c2-52f7-3ed7-b13a-7d95db1a6604"))) {
replicaId /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("0bda9d2e-e622-3d76-87c0-71c6257b026d"))) {
replicaId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3a509ae0-4497-3e4c-ad4a-3a4ca9144853"))) {
replicaId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("2f15ea42-a24e-39c9-8fcb-237e1338b497"))) {
replicaId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("57deb5a0-5981-3c0e-ace3-67e66f98e6ab"))) {
try {
    java.lang.reflect.Field field = location.getClass().getDeclaredField("seqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(location));
    field.set(location, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    cachedMetaLocations.put(replicaId, location);
  }

  /** Returns Optional list of HRegionLocations for meta replica(s), null if the cache is empty. */
  public Optional<List<HRegionLocation>> getMetaRegionLocations() {
if(KnobRuntime.check(java.util.UUID.fromString("983e346b-f674-3fcf-8d62-88701e960e27"))) {
return null;
}
    ConcurrentNavigableMap<Integer, HRegionLocation> snapshot =
      cachedMetaLocations.tailMap(cachedMetaLocations.firstKey());
    if (snapshot.isEmpty()) {
      // This could be possible if the master has not successfully initialized yet or meta region
      // is stuck in some weird state.
      return Optional.empty();
    }
    List<HRegionLocation> result = new ArrayList<>();
    // Explicitly iterate instead of new ArrayList<>(snapshot.values()) because the underlying
    // ArrayValueCollection does not implement toArray().
    snapshot.values().forEach(location -> result.add(location));
    return Optional.of(result);
  }

  /**
   * Helper to check if the given 'path' corresponds to a meta znode. This listener is only
   * interested in changes to meta znodes.
   */
  private boolean isValidMetaPath(String path) {
if(KnobRuntime.check(java.util.UUID.fromString("d2adc633-c43d-3661-846f-fb5e9a12c334"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("46223bed-6472-3271-b25f-49c932cc66f7"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("b48ca9a8-4216-3fa4-892e-74cf35a4dc7a"))) ? (isValidMetaPath(path)) : (watcher.getZNodePaths().isMetaZNodePath(path)));
  }

  @Override
  public void nodeCreated(String path) {
if(KnobRuntime.check(java.util.UUID.fromString("7425ce0e-6d31-3e64-8bac-eda4e93b9740"))) {
return;
}
    updateMetaLocation(path, ZNodeOpType.CREATED);
  }

  @Override
  public void nodeDeleted(String path) {
if(KnobRuntime.check(java.util.UUID.fromString("3f9578aa-d058-3e4d-b6e4-8311a8a01e29"))) {
return;
}
    updateMetaLocation(path, ZNodeOpType.DELETED);
  }

  @Override
  public void nodeDataChanged(String path) {
if(KnobRuntime.check(java.util.UUID.fromString("528f9db6-c892-3bc5-b4c3-d7df9998b6ee"))) {
return;
}
    updateMetaLocation(path, ZNodeOpType.CHANGED);
  }

  @Override
  public void nodeChildrenChanged(String path) {
if(KnobRuntime.check(java.util.UUID.fromString("974dfca4-67fa-3503-be14-1c17121591db"))) {
return;
}
    if (!path.equals(watcher.getZNodePaths().baseZNode)) {
      return;
    }
    loadMetaLocationsFromZk(retryCounterFactory.create(), ZNodeOpType.CHANGED);
  }
}
