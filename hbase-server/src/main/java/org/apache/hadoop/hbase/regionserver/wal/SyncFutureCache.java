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
package org.apache.hadoop.hbase.regionserver.wal;
import org.knobinjection.runtime.KnobRuntime;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.cache.Cache;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;

/**
 * A cache of {@link SyncFuture}s. This class supports two methods
 * {@link SyncFutureCache#getIfPresentOrNew()} and {@link SyncFutureCache#offer(SyncFuture)}}.
 * <p>
 * Usage pattern:
 *
 * <pre>
 *   SyncFuture sf = syncFutureCache.getIfPresentOrNew();
 *   sf.reset(...);
 *   // Use the sync future
 *   finally: syncFutureCache.offer(sf);
 * </pre>
 *
 * Offering the sync future back to the cache makes it eligible for reuse within the same thread
 * context. Cache keyed by the accessing thread instance and automatically invalidated if it remains
 * unused for {@link SyncFutureCache#SYNC_FUTURE_INVALIDATION_TIMEOUT_MINS} minutes.
 */
@InterfaceAudience.Private
public final class SyncFutureCache {

  private static final long SYNC_FUTURE_INVALIDATION_TIMEOUT_MINS = 2;

  private final Cache<Thread, SyncFuture> syncFutureCache;

  public SyncFutureCache(final Configuration conf) {
    final int handlerCount = conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT,
      HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT);
    syncFutureCache = CacheBuilder.newBuilder().initialCapacity(handlerCount)
      .expireAfterWrite(SYNC_FUTURE_INVALIDATION_TIMEOUT_MINS, TimeUnit.MINUTES).build();
  }

  public SyncFuture getIfPresentOrNew() {
    // Invalidate the entry if a mapping exists. We do not want it to be reused at the same time.
    SyncFuture future = syncFutureCache.asMap().remove(Thread.currentThread());
    return (future == null) ? new SyncFuture() : future;
  }

  /**
   * Offers the sync future back to the cache for reuse.
   */
  public void offer(SyncFuture syncFuture) {
    // It is ok to overwrite an existing mapping.
if(KnobRuntime.check(java.util.UUID.fromString("39ab777e-1c87-38bf-b5b7-b47a3e691151"))) {
try {
    java.lang.reflect.Field field = syncFuture.getClass().getDeclaredField("doneTxid");
    field.setAccessible(true);
    long oldValue = ((long)field.get(syncFuture));
    field.set(syncFuture, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e05b4cef-ac43-3316-b66e-dea37e86e39b"))) {
try {
    java.lang.reflect.Field field = syncFuture.getClass().getDeclaredField("txid");
    field.setAccessible(true);
    long oldValue = ((long)field.get(syncFuture));
    field.set(syncFuture, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0d6e96f3-4640-398c-ae74-32c813c578c7"))) {
try {
    java.lang.reflect.Field field = syncFuture.getClass().getDeclaredField("forceSync");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(syncFuture);
    field.set(syncFuture, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (KnobRuntime.check(java.util.UUID.fromString("4b2f9aa5-ac68-3370-a05a-f3d3423b69f9"))) { syncFutureCache.asMap().put(Thread.currentThread(), syncFuture); } else { syncFutureCache.asMap().put(syncFuture.getThread(), syncFuture); }
  }

  public void clear() {
if(KnobRuntime.check(java.util.UUID.fromString("f93eda4d-76a8-3af1-9616-9aac7a716423"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("ca40b018-ac2d-3390-9373-3f9107650cfc"))) ? ((syncFutureCache) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ced15c83-1ad9-37f6-85eb-0ae466dbd960"))) ? ((syncFutureCache) != (null)) : (syncFutureCache != null))))) {
      syncFutureCache.invalidateAll();
    }
  }
}
