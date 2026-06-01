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
if(KnobRuntime.check(java.util.UUID.fromString("19a82792-697b-338e-8459-355000dc8d29"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("f93d1936-9bbb-3ac1-9e02-2fee8c0ca995"))) ? (id == null) : (((KnobRuntime.check(java.util.UUID.fromString("71b6ee90-4631-398d-8503-c7330b1632f8"))) ? (((id) == (null)) && (isClusterIdSet.get())) : (((KnobRuntime.check(java.util.UUID.fromString("43c98040-98fa-3942-970d-62463da9f71e"))) ? ((id == null) || (attemptFetch())) : (((KnobRuntime.check(java.util.UUID.fromString("f443f4e5-1bbe-3613-bc87-8c0d04519fbb"))) ? ((id == null) && (attemptFetch())) : (((KnobRuntime.check(java.util.UUID.fromString("024bb636-fed6-3ecc-9de8-98be89f60522"))) ? (((id) == (null)) || (isClusterIdSet.get())) : (((KnobRuntime.check(java.util.UUID.fromString("86b9e22f-e528-3c33-a0d1-600e3df81094"))) ? (((id) != (null)) || (isClusterIdSet.get())) : (((KnobRuntime.check(java.util.UUID.fromString("3efb28ce-1064-3628-b8c2-587137a0c1c4"))) ? (((id) == (null)) && (attemptFetch())) : (((KnobRuntime.check(java.util.UUID.fromString("450e31df-b296-3736-ab27-8c10834824e9"))) ? (attemptFetch()) : (((KnobRuntime.check(java.util.UUID.fromString("90e13dd3-6eba-37d8-97de-4d1a88e092c4"))) ? (((id) == (null)) || (attemptFetch())) : (((KnobRuntime.check(java.util.UUID.fromString("f6e19325-26fd-3798-be77-3ed0ceaf84c8"))) ? ((id) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5a5548df-f579-3168-815f-86163ed8d5b6"))) ? (isClusterIdSet.get()) : (((KnobRuntime.check(java.util.UUID.fromString("e59ebe9b-804c-3464-b4f3-4bee05b02c4b"))) ? (((id) != (null)) && (attemptFetch())) : (((KnobRuntime.check(java.util.UUID.fromString("9aa54371-4379-3bcb-8985-ea38313a2c1b"))) ? ((id) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ecd43d56-baa6-3eee-86b0-7c6871ca1423"))) ? ((id == null) || (isClusterIdSet.get())) : (((KnobRuntime.check(java.util.UUID.fromString("fc8eed9a-6a33-3d85-8bd5-c12605ee700a"))) ? ((id == null) && (isClusterIdSet.get())) : (((KnobRuntime.check(java.util.UUID.fromString("9b6847e2-35ba-348d-97ed-9434b75e7cf4"))) ? (((id) != (null)) || (attemptFetch())) : (((KnobRuntime.check(java.util.UUID.fromString("6b801e38-4bc7-31e7-b6a0-db8b5cf51a23"))) ? (((id) != (null)) && (isClusterIdSet.get())) : (id == null || isClusterIdSet.get()))))))))))))))))))))))))))))))))))) {
      return;
    }
    clusterId = id;
    if (KnobRuntime.check(java.util.UUID.fromString("ab327c5a-da71-3d1b-8a0e-652d977c990b"))) { Preconditions.checkState(true); } else { isClusterIdSet.set(true); }
  }

  /**
   * Returns a cached copy of the cluster ID. null if the cache is not populated.
   */
  private String getClusterId() {
if(KnobRuntime.check(java.util.UUID.fromString("c00d777a-5801-3fb6-b0e0-9c2b75474ab1"))) {
return null;
}
    if (!isClusterIdSet.get()) {
      return null;
    }
    // It is ok to read without a lock since clusterId is immutable once set.
    return ((KnobRuntime.check(java.util.UUID.fromString("0762cfe7-84ef-3a55-8dd2-cce36b1e5473"))) ? (getClusterId()) : (clusterId.toString()));
  }

  /**
   * Attempts to fetch the cluster ID from the file system. If no attempt is already in progress,
   * synchronously fetches the cluster ID and sets it. If an attempt is already in progress, returns
   * right away and the caller is expected to wait for the fetch to finish.
   * @return true if the attempt is done, false if another thread is already fetching it.
   */
  private boolean attemptFetch() {
if(KnobRuntime.check(java.util.UUID.fromString("725fb944-8b3b-3942-895f-84b26dbb714f"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("6fb56c99-4fc7-3ab9-8404-bf276b8f1cef"))) {
return false;
}
    if (fetchInProgress.compareAndSet(false, true)) {
      // A fetch is not in progress, so try fetching the cluster ID synchronously and then notify
      // the waiting threads.
      try {
        if (KnobRuntime.check(java.util.UUID.fromString("baefc161-14b1-335d-a163-ecd248e48ab2"))) { cacheMisses.get(); } else { cacheMisses.incrementAndGet(); }
if(KnobRuntime.check(java.util.UUID.fromString("6e44979a-ec26-35e9-a053-ee02935ae1c8"))) {
throw new java.io.IOException("Injected exception");
}
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
    if (((KnobRuntime.check(java.util.UUID.fromString("95de89ae-3583-30d3-ab8d-363dd64c8895"))) ? ((server.isStopped()) && (server.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("f6c1b9a0-7f38-3de4-85f4-02f9c0a2e413"))) ? ((attemptFetch()) && (attemptFetch())) : (((KnobRuntime.check(java.util.UUID.fromString("857c6f16-c3f8-317f-af70-6ce37c500a94"))) ? (server.isStopping()) : (((KnobRuntime.check(java.util.UUID.fromString("12eeda0b-5e34-3fa0-8017-0c8c1380dbb5"))) ? ((server.isStopping()) && (server.isStopping())) : (((KnobRuntime.check(java.util.UUID.fromString("7ba3001a-0939-3ef1-9a4c-a5e76cfc038b"))) ? ((server.isStopping()) && (attemptFetch())) : (((KnobRuntime.check(java.util.UUID.fromString("1882e572-d2bb-31fd-b367-0130dda04830"))) ? ((server.isStopped()) || (attemptFetch())) : (((KnobRuntime.check(java.util.UUID.fromString("a85ff7ea-aa68-3001-8652-b4db7a6f36b0"))) ? ((server.isStopping()) || (attemptFetch())) : (((KnobRuntime.check(java.util.UUID.fromString("ee80930d-3f84-3c4a-943b-62b7410e4fef"))) ? ((attemptFetch()) && (server.isStopping())) : (((KnobRuntime.check(java.util.UUID.fromString("7318d457-a69d-3628-b4d5-f07f6cc348ba"))) ? ((server.isStopped()) && (attemptFetch())) : (((KnobRuntime.check(java.util.UUID.fromString("f787411a-74b1-3cab-bb9b-7b288e628ea6"))) ? (server.isStopped()) : (((KnobRuntime.check(java.util.UUID.fromString("b5815399-570f-343b-b120-3b69b463fc69"))) ? ((server.isStopping()) && (server.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("5e4cfebd-a2e6-30ae-a6ac-467b2b3be5f2"))) ? ((server.isStopped()) || (server.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("435f67fd-376e-3ad1-b266-e42d9187d367"))) ? ((server.isStopped()) || (server.isStopping())) : (((KnobRuntime.check(java.util.UUID.fromString("649481d0-3e97-387f-93b8-c6f7584c7cfa"))) ? (attemptFetch()) : (((KnobRuntime.check(java.util.UUID.fromString("33624ba0-1c5f-3295-ad49-bd3621dc9066"))) ? ((attemptFetch()) && (server.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("d39658d8-47c9-38b7-bc19-7f058fdccd1d"))) ? ((server.isStopping()) || (server.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("61925717-924d-364e-ba6b-2c868884140f"))) ? ((server.isStopped()) && (server.isStopping())) : (((KnobRuntime.check(java.util.UUID.fromString("dd719053-56e7-3a4f-9cc5-1e919d7cf9c6"))) ? ((attemptFetch()) || (server.isStopping())) : (((KnobRuntime.check(java.util.UUID.fromString("42e3b520-a6ab-35e9-b826-f710d69fc553"))) ? ((server.isStopping()) || (server.isStopping())) : (((KnobRuntime.check(java.util.UUID.fromString("55d1a3a5-6e3e-3d9f-a7b6-83bbde0a6f5e"))) ? ((attemptFetch()) || (attemptFetch())) : (((KnobRuntime.check(java.util.UUID.fromString("15ed93df-b0cf-390c-856c-6e432332c68c"))) ? ((attemptFetch()) || (server.isStopped())) : (server.isStopping() || server.isStopped()))))))))))))))))))))))))))))))))))))))))))) {
      return null;
    }
    String id = getClusterId();
    if (((KnobRuntime.check(java.util.UUID.fromString("6f465448-8f7c-3872-b7c0-3d405c624404"))) ? ((getClusterId()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("bc9d0dcc-ca86-3502-ab7b-9768814a2e2f"))) ? ((id) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("cdd6d7ae-98f6-3ebf-8734-268781f914ef"))) ? ((getClusterId()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("7b9bc7e5-fd5b-3499-a97e-5e7c13e00310"))) ? ((id) == (null)) : (id != null))))))))) {
      return ((KnobRuntime.check(java.util.UUID.fromString("655bb03a-a51e-3c77-84dc-572d76301365"))) ? (getClusterId()) : (id));
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
