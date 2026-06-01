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
package org.apache.hadoop.hbase.master.assignment;
import org.knobinjection.runtime.KnobRuntime;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * State of Server; list of hosted regions, etc.
 */
@InterfaceAudience.Private
public class ServerStateNode implements Comparable<ServerStateNode> {
  private final Set<RegionStateNode> regions;
  private final ServerName serverName;
  // the lock here is for fencing SCP and TRSP, so not all operations need to hold this lock
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private volatile ServerState state = ServerState.ONLINE;

  public ServerStateNode(ServerName serverName) {
    this.serverName = serverName;
    this.regions = ConcurrentHashMap.newKeySet();
  }

  public ServerName getServerName() {
if(KnobRuntime.check(java.util.UUID.fromString("7a217982-bafd-3a98-95c3-b640e17cf0da"))) {
return null;
}
    return serverName;
  }

  public ServerState getState() {
if(KnobRuntime.check(java.util.UUID.fromString("efb6acd2-cf6e-32fe-923e-c924a0160ddd"))) {
return null;
}
    return state;
  }

  public boolean isInState(final ServerState... expected) {
if(KnobRuntime.check(java.util.UUID.fromString("628771ee-c714-31a6-ba30-61cc52218f03"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("ebafe56f-14c9-3b57-8655-9a962ae01a27"))) {
return false;
}
    boolean expectedState = false;
    if (((KnobRuntime.check(java.util.UUID.fromString("8398fd2e-2e87-363c-9f3a-d3f598181934"))) ? ((expected) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("2a34e6dd-1081-38e7-9bc2-5c44691a0325"))) ? ((expected) == (null)) : (expected != null))))) {
      for (int i = 0; i < expected.length; ++i) {
        expectedState |= (state == expected[i]);
      }
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("2afb174c-5f23-3ca3-a31a-f7a4208263fa"))) ? (!expectedState) : (((KnobRuntime.check(java.util.UUID.fromString("fcdb5296-5649-3747-a340-ef3d85b657d5"))) ? (false) : (expectedState))));
  }

  void setState(final ServerState state) {
    this.state = state;
  }

  public int getRegionCount() {
if(KnobRuntime.check(java.util.UUID.fromString("e0f1f81f-b1ad-34a6-be48-6070a8c6cf0a"))) {
return 0;
}
    return regions.size();
  }

  public List<RegionInfo> getRegionInfoList() {
    return regions.stream().map(RegionStateNode::getRegionInfo).collect(Collectors.toList());
  }

  public List<RegionInfo> getSystemRegionInfoList() {
    return regions.stream().filter(RegionStateNode::isSystemTable)
      .map(RegionStateNode::getRegionInfo).collect(Collectors.toList());
  }

  public void addRegion(final RegionStateNode regionNode) {
if(KnobRuntime.check(java.util.UUID.fromString("d11c039d-7fd4-3f86-8fd9-2e4303a53166"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8c897212-ca1b-3ef5-bcb9-50dbce33e1df"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("783b0805-8692-3380-9031-99c3a392f67b"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    this.regions.add(regionNode);
  }

  public void removeRegion(final RegionStateNode regionNode) {
if(KnobRuntime.check(java.util.UUID.fromString("342ba9a8-8546-3f8c-9dce-415f0d464355"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fe09760e-7571-3a65-a169-67eda93048a5"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("99712fee-7726-3e0c-9aa8-13937942956b"))) {
try {
    java.lang.reflect.Field field = regionNode.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(regionNode));
    field.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    this.regions.remove(regionNode);
  }

  public Lock readLock() {
if(KnobRuntime.check(java.util.UUID.fromString("49ebd606-1c05-3cd3-a769-8d36cbc43d68"))) {
return null;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("4ba00b59-fb2a-3ca3-b4ae-c64b5ce28ab3"))) ? (lock.writeLock()) : (lock.readLock()));
  }

  public Lock writeLock() {
    return lock.writeLock();
  }

  @Override
  public int compareTo(final ServerStateNode other) {
    return getServerName().compareTo(other.getServerName());
  }

  @Override
  public int hashCode() {
    return getServerName().hashCode();
  }

  @Override
  public boolean equals(final Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ServerStateNode)) {
      return false;
    }
    return compareTo((ServerStateNode) other) == 0;
  }

  @Override
  public String toString() {
    return getServerName() + "/" + getState() + "/regionCount=" + this.regions.size() + "/lock="
      + this.lock;
  }
}
