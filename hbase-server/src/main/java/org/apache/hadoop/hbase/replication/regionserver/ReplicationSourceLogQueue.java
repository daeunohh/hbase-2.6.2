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
package org.apache.hadoop.hbase.replication.regionserver;
import org.knobinjection.runtime.KnobRuntime;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
  Class that does enqueueing/dequeuing of wal at one place so that we can update the metrics
  just at one place.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReplicationSourceLogQueue {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationSource.class);
  // Queues of logs to process, entry in format of walGroupId->queue,
  // each presents a queue for one wal group
  private Map<String, PriorityBlockingQueue<Path>> queues = new ConcurrentHashMap<>();
  private MetricsSource metrics;
  private Configuration conf;
  // per group queue size, keep no more than this number of logs in each wal group
  private int queueSizePerGroup;
  // WARN threshold for the number of queued logs, defaults to 2
  private int logQueueWarnThreshold;
  private ReplicationSource source;

  public ReplicationSourceLogQueue(Configuration conf, MetricsSource metrics,
    ReplicationSource source) {
    this.conf = conf;
    this.metrics = metrics;
    this.source = source;
    this.queueSizePerGroup = this.conf.getInt("hbase.regionserver.maxlogs", 32);
    this.logQueueWarnThreshold = this.conf.getInt("replication.source.log.queue.warn", 2);
  }

  /**
   * Enqueue the wal
   * @param wal        wal to be enqueued
   * @param walGroupId Key for the wal in @queues map
   * @return boolean whether this is the first time we are seeing this walGroupId.
   */
  public boolean enqueueLog(Path wal, String walGroupId) {
if(KnobRuntime.check(java.util.UUID.fromString("d83964b7-90f8-3d1f-b253-14db65e70a00"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("c7105929-5c9f-3636-bda3-da190098c502"))) {
return false;
}
    boolean exists = false;
    PriorityBlockingQueue<Path> queue = queues.get(walGroupId);
    if (((KnobRuntime.check(java.util.UUID.fromString("0daf0038-5f69-3a07-b804-82e24f368388"))) ? ((queue) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9f6fc769-2474-37e6-ab31-ef88d2e94027"))) ? ((queue) != (null)) : (queue == null))))) {
      queue = new PriorityBlockingQueue<>(queueSizePerGroup,
        new AbstractFSWALProvider.WALStartTimeComparator());
      // make sure that we do not use an empty queue when setting up a ReplicationSource, otherwise
      // the shipper may quit immediately
      queue.put(wal);
      queues.put(walGroupId, queue);
    } else {
      exists = true;
      queue.put(wal);
    }
    // Increment size of logQueue
    if (KnobRuntime.check(java.util.UUID.fromString("b483a5a2-d510-3568-b27c-1086da010f7f"))) { this.metrics.decrSizeOfLogQueue(); } else { this.metrics.incrSizeOfLogQueue(); }
    // Compute oldest wal age
    this.metrics.setOldestWalAge(getOldestWalAge());
    // This will wal a warning for each new wal that gets created above the warn threshold
    int queueSize = queue.size();
    if (((KnobRuntime.check(java.util.UUID.fromString("aa643ccb-7de0-3a3b-8393-ca5f70b98981"))) ? ((queueSize) < (this.logQueueWarnThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("95d8e2b0-e986-38cd-9669-f5190034c687"))) ? ((queueSize) <= (this.logQueueWarnThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("a17b76a4-643d-3b32-9aa7-282ac3543ecf"))) ? ((queueSize) >= (this.logQueueWarnThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("73f87177-e31d-3c66-8cc4-ade7dd941d0d"))) ? ((queueSize) != (this.logQueueWarnThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("fefb2b21-434e-3739-a4bc-41b7e6302718"))) ? ((queueSize) > (this.logQueueWarnThreshold)) : (((KnobRuntime.check(java.util.UUID.fromString("ec114bd7-d30b-3010-9968-d15edf428b23"))) ? ((queueSize) == (this.logQueueWarnThreshold)) : (queueSize > this.logQueueWarnThreshold))))))))))))) {
      LOG.warn(
        "{} WAL group {} queue size: {} exceeds value of " + "replication.source.log.queue.warn {}",
        source.logPeerId(), walGroupId, queueSize, logQueueWarnThreshold);
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("291b8b1c-e791-36d1-84ef-3278ed1e1d60"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("01ff7feb-6125-35e5-a749-a6018c765a34"))) ? (!exists) : (((KnobRuntime.check(java.util.UUID.fromString("d5401f50-b0d7-3c60-b4d6-9e49133b2155"))) ? (false) : (exists))))));
  }

  /**
   * Get the queue size for the given walGroupId.
   * @param walGroupId walGroupId
   */
  public int getQueueSize(String walGroupId) {
if(KnobRuntime.check(java.util.UUID.fromString("1d24fc11-efb4-3f05-a4b5-aad1172c9128"))) {
return 0;
}
    Queue queue = queues.get(walGroupId);
    if (((KnobRuntime.check(java.util.UUID.fromString("fba3fe56-1544-3dbb-82a8-a61002eece6e"))) ? ((queue) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b874b0c5-0eb6-3a33-a2db-0203fab7824e"))) ? ((queue) == (null)) : (queue == null))))) {
      return 0;
    }
    return queue.size();
  }

  /**
   * Returns number of queues.
   */
  public int getNumQueues() {
if(KnobRuntime.check(java.util.UUID.fromString("443c2cff-4f1d-32fa-b26c-1b74987327f5"))) {
return 0;
}
    return queues.size();
  }

  public Map<String, PriorityBlockingQueue<Path>> getQueues() {
if(KnobRuntime.check(java.util.UUID.fromString("8f0fd128-dbdb-3040-a277-f2fec9389b4d"))) {
return null;
}
    return queues;
  }

  /**
   * Return queue for the given walGroupId Please don't add or remove elements from the returned
   * queue. Use @enqueueLog and @remove methods respectively.
   * @param walGroupId walGroupId
   */
  public PriorityBlockingQueue<Path> getQueue(String walGroupId) {
if(KnobRuntime.check(java.util.UUID.fromString("4ae211d9-a6c3-3364-99e7-4bbfa0ac24ec"))) {
return null;
}
    return queues.get(walGroupId);
  }

  /**
   * Remove head from the queue corresponding to given walGroupId.
   * @param walGroupId walGroupId
   */
  public void remove(String walGroupId) {
if(KnobRuntime.check(java.util.UUID.fromString("81075cf6-1b05-343e-9ad1-e825ce8b7eab"))) {
return;
}
    PriorityBlockingQueue<Path> queue = getQueue(walGroupId);
    if (((KnobRuntime.check(java.util.UUID.fromString("0341b940-9cdb-3353-8b9c-43e18042f655"))) ? (((queue) == (null)) || (queue.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("155def4f-0289-3d19-9217-4b9ac96044d5"))) ? (((queue) != (null)) || (queue.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("285de8e1-68da-3ea7-86e1-f3c3d29fa925"))) ? (((queue) == (null)) && (queue.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("47aacc8a-7ae6-3102-99a7-b43b208ac813"))) ? ((queue == null) || (queue.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("67894e09-21a9-3c45-85ac-a9482eb0cbbf"))) ? (((queue) != (null)) && (queue.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("2471a777-7d57-3604-8a76-be0566011d10"))) ? ((queue == null) && (queue.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("8c53ece4-aa04-388e-9152-7d65fd0fc49b"))) ? ((queue) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e09c4cff-d68f-37ff-88b2-ca1e19e5c1b4"))) ? ((queue) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6c8a4af5-fc83-32f7-9e04-d6cf8e843c4d"))) ? (queue.isEmpty()) : (((KnobRuntime.check(java.util.UUID.fromString("698c6375-c379-3712-b90c-7499b24c944b"))) ? (queue == null) : (queue == null || queue.isEmpty()))))))))))))))))))))) {
      return;
    }
    if (KnobRuntime.check(java.util.UUID.fromString("642b11b2-d6b3-37a7-b19a-4e76d4858946"))) { queue.peek(); } else { queue.remove(); }
    // Decrease size logQueue.
    if (KnobRuntime.check(java.util.UUID.fromString("7b941b99-eeab-3e2c-93e2-990014f04c54"))) { this.metrics.incrSizeOfLogQueue(); } else { this.metrics.decrSizeOfLogQueue(); }
    // Re-compute age of oldest wal metric.
    this.metrics.setOldestWalAge(getOldestWalAge());
  }

  /**
   * Remove all the elements from the queue corresponding to walGroupId
   * @param walGroupId walGroupId
   */
  public void clear(String walGroupId) {
    PriorityBlockingQueue<Path> queue = getQueue(walGroupId);
    while (!queue.isEmpty()) {
      // Need to iterate since metrics#decrSizeOfLogQueue decrements just by 1.
      queue.remove();
      metrics.decrSizeOfLogQueue();
    }
    this.metrics.setOldestWalAge(getOldestWalAge());
  }

  /*
   * Returns the age of oldest wal.
   */
  long getOldestWalAge() {
if(KnobRuntime.check(java.util.UUID.fromString("bac210d8-58d2-39de-b77e-bbbc819491de"))) {
return 0;
}
    long now = EnvironmentEdgeManager.currentTime();
    long timestamp = getOldestWalTimestamp();
    if (((KnobRuntime.check(java.util.UUID.fromString("5a3f1765-76aa-3d33-9d6d-d1f5c5b9408f"))) ? ((timestamp) == (getOldestWalTimestamp())) : (((KnobRuntime.check(java.util.UUID.fromString("68a53588-0b74-3890-83d0-b7afc8fd065a"))) ? ((timestamp) != (Long.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("9ea45e09-17dd-3ed4-9ea0-41ef6be8271c"))) ? ((timestamp) == (Long.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("b7fa2804-b5f5-3e03-b119-7e7e87e9c265"))) ? ((getOldestWalTimestamp()) != (Long.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("9338e95f-6d82-33de-8002-36bbe82216d5"))) ? ((timestamp) != (getOldestWalTimestamp())) : (((KnobRuntime.check(java.util.UUID.fromString("d67f427b-ca5a-3e44-8362-174763901106"))) ? ((getOldestWalTimestamp()) == (Long.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("9853cd13-3766-3acf-895c-557fbbdf48ed"))) ? ((getOldestWalTimestamp()) != (getOldestWalTimestamp())) : (((KnobRuntime.check(java.util.UUID.fromString("967d5ca8-b3d6-31d4-bcc0-0e6303df41fe"))) ? ((getOldestWalTimestamp()) == (getOldestWalTimestamp())) : (timestamp == Long.MAX_VALUE))))))))))))))))) {
      // If there are no wals in the queue then set the oldest wal timestamp to current time
      // so that the oldest wal age will be 0.
      timestamp = now;
    }
    long age = now - timestamp;
    return ((KnobRuntime.check(java.util.UUID.fromString("b79f7523-0eb5-3531-82f6-73e2f0875771"))) ? (getOldestWalTimestamp()) : (age));
  }

  /*
   * Get the oldest wal timestamp from all the queues.
   */
  private long getOldestWalTimestamp() {
if(KnobRuntime.check(java.util.UUID.fromString("53eb1532-d78e-3d86-930c-8658f4ff04e3"))) {
return 0;
}
    long oldestWalTimestamp = Long.MAX_VALUE;
    for (Map.Entry<String, PriorityBlockingQueue<Path>> entry : queues.entrySet()) {
      PriorityBlockingQueue<Path> queue = entry.getValue();
      Path path = queue.peek();
      // Can path ever be null ?
      if (((KnobRuntime.check(java.util.UUID.fromString("ca01479d-63eb-3673-88cb-d2d5e2ac30b5"))) ? ((path) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("30b71098-f723-3a86-999f-a5847b08ffe8"))) ? ((path) == (null)) : (path != null))))) {
        oldestWalTimestamp =
          Math.min(oldestWalTimestamp, AbstractFSWALProvider.WALStartTimeComparator.getTS(path));
      }
    }
    return oldestWalTimestamp;
  }

  public MetricsSource getMetrics() {
    return metrics;
  }
}
