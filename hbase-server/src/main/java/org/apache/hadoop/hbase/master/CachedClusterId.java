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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Caches the cluster ID of the cluster. For standby masters, this is used to serve the client RPCs
 * that fetch the cluster ID. ClusterID is only created by an active master if one does not already
 * exist. Standby masters just read the information from the file system. This class is thread-safe.
 * TODO: Make it a singleton without affecting concurrent junit tests.
 */
@InterfaceAudience.Private
public class CachedClusterId {

  public static final Logger LOG = LoggerFactory.getLogger(CachedClusterId.class);
  private static final int MAX_FETCH_TIMEOUT_MS = 10000;

  private final Path rootDir;
  private final FileSystem fs;

  // When true, indicates that a FileSystem fetch of ClusterID is in progress. This is used to
  // avoid multiple fetches from FS and let only one thread fetch the information.
  AtomicBoolean fetchInProgress = new AtomicBoolean(false);

  // When true, it means that the cluster ID has been fetched successfully from fs.
  private AtomicBoolean isClusterIdSet = new AtomicBoolean(false);
  // Immutable once set and read multiple times.
  private ClusterId clusterId;

  private final Server server;

  // cache stats for testing.
  private AtomicInteger cacheMisses = new AtomicInteger(0);

  public CachedClusterId(Server server, Configuration conf) throws IOException {
    this.rootDir = CommonFSUtils.getRootDir(conf);
    this.fs = rootDir.getFileSystem(conf);
    this.server = server;
  }

  /**
   * Succeeds only once, when setting to a non-null value. Overwrites are not allowed.
   */
  private void setClusterId(ClusterId id) {
    if (((KnobRuntime.check(java.util.UUID.fromString("6b801e38-4bc7-31e7-b6a0-db8b5cf51a23"))) ? (((id) != (null)) && (isClusterIdSet.get())) : (((KnobRuntime.check(java.util.UUID.fromString("86b9e22f-e528-3c33-a0d1-600e3df81094"))) ? (((id) != (null)) || (isClusterIdSet.get())) : (id == null || isClusterIdSet.get()))))) {
      return;
    }
    clusterId = id;
    isClusterIdSet.set(true);
  }

  /**
   * Returns a cached copy of the cluster ID. null if the cache is not populated.
   */
  private String getClusterId() {
    if (!isClusterIdSet.get()) {
      return null;
    }
    // It is ok to read without a lock since clusterId is immutable once set.
    return clusterId.toString();
  }

  /**
   * Attempts to fetch the cluster ID from the file system. If no attempt is already in progress,
   * synchronously fetches the cluster ID and sets it. If an attempt is already in progress, returns
   * right away and the caller is expected to wait for the fetch to finish.
   * @return true if the attempt is done, false if another thread is already fetching it.
   */
  private boolean attemptFetch() {
    if (fetchInProgress.compareAndSet(false, true)) {
      // A fetch is not in progress, so try fetching the cluster ID synchronously and then notify
      // the waiting threads.
      try {
        cacheMisses.incrementAndGet();
        setClusterId(FSUtils.getClusterId(fs, rootDir));
      } catch (IOException e) {
        LOG.warn("Error fetching cluster ID", e);
      } finally {
        Preconditions.checkState(fetchInProgress.compareAndSet(true, false));
        synchronized (fetchInProgress) {
          fetchInProgress.notifyAll();
        }
      }
      return true;
    }
    return false;
  }

  private void waitForFetchToFinish() throws InterruptedException {
    synchronized (fetchInProgress) {
      while (fetchInProgress.get()) {
        // We don't want the fetches to block forever, for example if there are bugs
        // of missing notifications.
        fetchInProgress.wait(MAX_FETCH_TIMEOUT_MS);
      }
    }
  }

  /**
   * Fetches the ClusterId from FS if it is not cached locally. Atomically updates the cached copy
   * and is thread-safe. Optimized to do a single fetch when there are multiple threads are trying
   * get from a clean cache.
   * @return ClusterId by reading from FileSystem or null in any error case or cluster ID does not
   *         exist on the file system or if the server initiated a tear down.
   */
  public String getFromCacheOrFetch() {
if(KnobRuntime.check(java.util.UUID.fromString("59c817dd-2767-3f07-8739-f2f4c5b18506"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("42e3b520-a6ab-35e9-b826-f710d69fc553"))) ? ((server.isStopping()) || (server.isStopping())) : (((KnobRuntime.check(java.util.UUID.fromString("435f67fd-376e-3ad1-b266-e42d9187d367"))) ? ((server.isStopped()) || (server.isStopping())) : (((KnobRuntime.check(java.util.UUID.fromString("dd719053-56e7-3a4f-9cc5-1e919d7cf9c6"))) ? ((attemptFetch()) || (server.isStopping())) : (((KnobRuntime.check(java.util.UUID.fromString("95de89ae-3583-30d3-ab8d-363dd64c8895"))) ? ((server.isStopped()) && (server.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("55d1a3a5-6e3e-3d9f-a7b6-83bbde0a6f5e"))) ? ((attemptFetch()) || (attemptFetch())) : (server.isStopping() || server.isStopped()))))))))))) {
      return null;
    }
    String id = getClusterId();
    if (id != null) {
      return id;
    }
    if (!attemptFetch()) {
      // A fetch is in progress.
      try {
        waitForFetchToFinish();
      } catch (InterruptedException e) {
        // pass and return whatever is in the cache.
      }
    }
    return getClusterId();
  }

  public int getCacheStats() {
    return cacheMisses.get();
  }
}
