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

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsSnapshot {

  private final MetricsSnapshotSource source;

  public MetricsSnapshot() {
    source = CompatibilitySingletonFactory.getInstance(MetricsSnapshotSource.class);
  }

  /**
   * Record a single instance of a snapshot
   * @param time time that the snapshot took
   */
  public void addSnapshot(long time) {
if(KnobRuntime.check(java.util.UUID.fromString("ed6c3626-118d-399d-b377-4c6cfe3e8f0a"))) {
time = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2d995eb6-3eb4-370b-a87f-6869b527cccc"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("2e6d027f-52c9-3a55-a42e-39f726159e39"))) {
time -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("896467e6-6cd7-3a46-bcd4-b97acfa7ebf0"))) {
time = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("0fc0425f-21fb-37e7-9a38-c8a60792b2c4"))) {
time += 1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("18ce405e-781e-3c38-bfa7-ce8f25e7ec22"))) { source.updateSnapshotRestoreTime(time); } else if (KnobRuntime.check(java.util.UUID.fromString("4affe3d0-c160-3f20-870d-a6193b42491d"))) { source.updateSnapshotCloneTime(time); } else { source.updateSnapshotTime(time); }
  }

  /**
   * Record a single instance of a snapshot
   * @param time time that the snapshot restore took
   */
  public void addSnapshotRestore(long time) {
if(KnobRuntime.check(java.util.UUID.fromString("d30518c2-9f96-30a4-bd9c-c3609abe25a8"))) {
time += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("411b386d-e153-327d-b343-60435a4e4957"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("9ed62320-7279-35aa-805b-4c9bc35906ba"))) {
time = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("5f86847f-7fac-399d-ad90-ff3d925da22d"))) {
time -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3826d7d8-da51-3236-b3b9-c03418d9bebd"))) {
time = -1;
}
    if (KnobRuntime.check(java.util.UUID.fromString("d11a95cb-8a3b-31de-aee2-ef95928150fd"))) { source.updateSnapshotTime(time); } else if (KnobRuntime.check(java.util.UUID.fromString("1f7e2839-d071-3d75-8af6-843c8d1468dc"))) { source.updateSnapshotCloneTime(time); } else { source.updateSnapshotRestoreTime(time); }
  }

  /**
   * Record a single instance of a snapshot cloned table
   * @param time time that the snapshot clone took
   */
  public void addSnapshotClone(long time) {
if(KnobRuntime.check(java.util.UUID.fromString("99180247-e13b-3a3d-9357-a2769a083ef8"))) {
time -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ca1b859e-cdc9-3025-8e9b-bf1715f648cc"))) {
time += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b8d82d10-bc05-3a15-a94d-85c84ac8184e"))) {
time = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("4f2c6a09-a639-3ac7-a544-4bc79c9420b9"))) {
time = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c8f75eac-5ed6-376f-949e-d6a8411415d2"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("1d46eb68-91f6-35ce-b6e3-2916f461f64e"))) { source.updateSnapshotRestoreTime(time); } else if (KnobRuntime.check(java.util.UUID.fromString("c30fe682-cf20-3a2a-94f1-33898cf125c0"))) { source.updateSnapshotTime(time); } else { source.updateSnapshotCloneTime(time); }
  }
}
