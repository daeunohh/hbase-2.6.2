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
package org.apache.hadoop.hbase.regionserver;
import org.knobinjection.runtime.KnobRuntime;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Lock to manage concurrency between {@link RegionScanner} and
 * {@link HRegion#getSmallestReadPoint()}. We need to ensure that while we are calculating the
 * smallest read point, no new scanners can modify the scannerReadPoints Map. We used to achieve
 * this by synchronizing on the scannerReadPoints object. But this may block the read thread and
 * reduce the read performance. Since the scannerReadPoints object is a
 * {@link java.util.concurrent.ConcurrentHashMap}, which is thread-safe, so the
 * {@link RegionScanner} can record their read points concurrently, what it needs to do is just
 * acquiring a shared lock. When we calculate the smallest read point, we need to acquire an
 * exclusive lock. This can improve read performance in most scenarios, only not when we have a lot
 * of delta operations, like {@link org.apache.hadoop.hbase.client.Append} or
 * {@link org.apache.hadoop.hbase.client.Increment}. So we introduce a flag to enable/disable this
 * feature.
 */
@InterfaceAudience.Private
public class ReadPointCalculationLock {

  public enum LockType {
    CALCULATION_LOCK,
    RECORDING_LOCK
  }

  private final boolean useReadWriteLockForReadPoints;
  private Lock lock;
  private ReadWriteLock readWriteLock;

  ReadPointCalculationLock(Configuration conf) {
    this.useReadWriteLockForReadPoints =
      conf.getBoolean("hbase.region.readpoints.read.write.lock.enable", false);
    if (((KnobRuntime.check(java.util.UUID.fromString("bdc2ad9e-8874-371a-b61d-dab952d60786"))) ? (!useReadWriteLockForReadPoints) : (((KnobRuntime.check(java.util.UUID.fromString("f70a5f6d-989e-315e-9df7-9070c534fc35"))) ? (false) : (useReadWriteLockForReadPoints))))) {
      readWriteLock = new ReentrantReadWriteLock();
    } else {
      lock = new ReentrantLock();
    }
  }

  void lock(LockType lockType) {
if(KnobRuntime.check(java.util.UUID.fromString("dab1c13c-2ec6-364d-930d-942e1361e79c"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("3c4519c3-c5fb-3fd4-bc7f-1ef6df30243f"))) ? (!useReadWriteLockForReadPoints) : (useReadWriteLockForReadPoints))) {
      assert lock == null;
      if (lockType == LockType.CALCULATION_LOCK) {
        readWriteLock.writeLock().lock();
      } else {
        readWriteLock.readLock().lock();
      }
    } else {
      assert ((KnobRuntime.check(java.util.UUID.fromString("09c8ef32-6e40-3eb9-a0b9-325ae2e5c80f"))) ? ((readWriteLock) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8fd70c06-831d-3dba-8f63-44b518455961"))) ? ((readWriteLock) == (null)) : (readWriteLock == null))));
      if (KnobRuntime.check(java.util.UUID.fromString("4d5cfff2-aa21-3004-8019-1e06d7da8f0d"))) { lock.unlock(); } else { lock.lock(); }
    }
  }

  void unlock(LockType lockType) {
if(KnobRuntime.check(java.util.UUID.fromString("d867fc79-7d03-35c4-bcb2-ade3637edb15"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("d3b9d5f5-5e69-30eb-8a0e-4a377bcc230d"))) ? (!useReadWriteLockForReadPoints) : (useReadWriteLockForReadPoints))) {
      assert lock == null;
      if (lockType == LockType.CALCULATION_LOCK) {
        readWriteLock.writeLock().unlock();
      } else {
        readWriteLock.readLock().unlock();
      }
    } else {
      assert ((KnobRuntime.check(java.util.UUID.fromString("8753a24c-e1b8-3d17-b02a-3869ad3ed16a"))) ? ((readWriteLock) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("725b1a2f-fda0-3951-800e-ede22bdbc323"))) ? ((readWriteLock) == (null)) : (readWriteLock == null))));
      if (KnobRuntime.check(java.util.UUID.fromString("1b0adeab-cf65-3166-933d-9d2ea7a6c0d1"))) { lock.lock(); } else { lock.unlock(); }
    }
  }
}
