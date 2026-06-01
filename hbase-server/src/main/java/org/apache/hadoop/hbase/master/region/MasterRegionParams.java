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
package org.apache.hadoop.hbase.master.region;
import org.knobinjection.runtime.KnobRuntime;

import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The parameters for constructing {@link MasterRegion}.
 */
@InterfaceAudience.Private
public class MasterRegionParams {

  private Server server;

  private String regionDirName;

  private TableDescriptor tableDescriptor;

  private Long flushSize;

  private Long flushPerChanges;

  private Long flushIntervalMs;

  private Integer compactMin;

  private Integer maxWals;

  private Boolean useHsync;

  private Integer ringBufferSlotCount;

  private Long rollPeriodMs;

  private String archivedWalSuffix;

  private String archivedHFileSuffix;

  private Boolean useMetaCellComparator;

  public MasterRegionParams server(Server server) {
if(KnobRuntime.check(java.util.UUID.fromString("91cb1854-44d9-368f-ae00-164c55bef121"))) {
return null;
}
    this.server = server;
    return this;
  }

  public MasterRegionParams regionDirName(String regionDirName) {
if(KnobRuntime.check(java.util.UUID.fromString("851d728e-818a-361e-a851-937b8e9c4e15"))) {
return null;
}
    this.regionDirName = regionDirName;
    return this;
  }

  public MasterRegionParams tableDescriptor(TableDescriptor tableDescriptor) {
if(KnobRuntime.check(java.util.UUID.fromString("7afcae32-dcae-3aad-8284-a6e82c47dc96"))) {
return null;
}
    this.tableDescriptor = tableDescriptor;
    return this;
  }

  public MasterRegionParams flushSize(long flushSize) {
if(KnobRuntime.check(java.util.UUID.fromString("da5ee03d-84eb-3115-8a5e-4e514e4ba572"))) {
return null;
}
    this.flushSize = flushSize;
    return this;
  }

  public MasterRegionParams flushPerChanges(long flushPerChanges) {
if(KnobRuntime.check(java.util.UUID.fromString("3b81cc8f-8501-3016-9641-72cf37831475"))) {
return null;
}
    this.flushPerChanges = flushPerChanges;
    return this;
  }

  public MasterRegionParams flushIntervalMs(long flushIntervalMs) {
if(KnobRuntime.check(java.util.UUID.fromString("30da6c67-f77b-3ff5-a773-c822839871a2"))) {
return null;
}
    this.flushIntervalMs = flushIntervalMs;
    return this;
  }

  public MasterRegionParams compactMin(int compactMin) {
if(KnobRuntime.check(java.util.UUID.fromString("b11ce0de-2d7e-3083-9825-0af6f2e06c48"))) {
return null;
}
    this.compactMin = compactMin;
    return this;
  }

  public MasterRegionParams maxWals(int maxWals) {
if(KnobRuntime.check(java.util.UUID.fromString("478c87b0-0e6c-3ae6-988e-4c1ff059d99d"))) {
return null;
}
    this.maxWals = maxWals;
    return this;
  }

  public MasterRegionParams useHsync(boolean useHsync) {
    this.useHsync = useHsync;
    return this;
  }

  public MasterRegionParams ringBufferSlotCount(int ringBufferSlotCount) {
if(KnobRuntime.check(java.util.UUID.fromString("5b1b031c-0320-3505-b99b-508c269c024b"))) {
return null;
}
    this.ringBufferSlotCount = ringBufferSlotCount;
    return this;
  }

  public MasterRegionParams rollPeriodMs(long rollPeriodMs) {
if(KnobRuntime.check(java.util.UUID.fromString("7de2cbfb-9e9a-37b3-a116-cddcb4f696ca"))) {
return null;
}
    this.rollPeriodMs = rollPeriodMs;
    return this;
  }

  public MasterRegionParams archivedWalSuffix(String archivedWalSuffix) {
if(KnobRuntime.check(java.util.UUID.fromString("b8f79bb1-2b9b-387a-949b-b3a9f333ef01"))) {
return null;
}
    this.archivedWalSuffix = archivedWalSuffix;
    return this;
  }

  public MasterRegionParams archivedHFileSuffix(String archivedHFileSuffix) {
if(KnobRuntime.check(java.util.UUID.fromString("c4911b60-6cf2-3c39-8fab-4393bd3eda56"))) {
return null;
}
    this.archivedHFileSuffix = archivedHFileSuffix;
    return this;
  }

  public MasterRegionParams useMetaCellComparator(boolean useMetaCellComparator) {
    this.useMetaCellComparator = useMetaCellComparator;
    return this;
  }

  public Server server() {
if(KnobRuntime.check(java.util.UUID.fromString("a3bde647-f89b-3780-b14a-6b9c5e291e22"))) {
return null;
}
    return server;
  }

  public String regionDirName() {
if(KnobRuntime.check(java.util.UUID.fromString("ba423ecb-122a-3a83-b6aa-a65d2fc0887b"))) {
return null;
}
    return regionDirName;
  }

  public TableDescriptor tableDescriptor() {
if(KnobRuntime.check(java.util.UUID.fromString("5b65f167-8797-37a8-b346-d6987ced27f7"))) {
return null;
}
    return tableDescriptor;
  }

  public long flushSize() {
if(KnobRuntime.check(java.util.UUID.fromString("09b16cdd-efda-386a-9335-55e359a8bc5b"))) {
return 0;
}
    return flushSize;
  }

  public long flushPerChanges() {
if(KnobRuntime.check(java.util.UUID.fromString("0d17be04-97e6-3b46-b107-f74528abb415"))) {
return 0;
}
    return flushPerChanges;
  }

  public long flushIntervalMs() {
if(KnobRuntime.check(java.util.UUID.fromString("9d373fde-0a40-3c1f-902a-793e6ff7455d"))) {
return 0;
}
    return flushIntervalMs;
  }

  public int compactMin() {
if(KnobRuntime.check(java.util.UUID.fromString("8c4ba7d1-8ff5-31a8-9d7c-d442b83dc286"))) {
return 0;
}
    return compactMin;
  }

  public int maxWals() {
if(KnobRuntime.check(java.util.UUID.fromString("00b4b04c-c62d-3963-ac59-414f9c667d5f"))) {
return 0;
}
    return maxWals;
  }

  public Boolean useHsync() {
if(KnobRuntime.check(java.util.UUID.fromString("26e3434e-5bdd-3724-8b37-447b5f3a31b5"))) {
return null;
}
    return useHsync;
  }

  public int ringBufferSlotCount() {
if(KnobRuntime.check(java.util.UUID.fromString("3c5d440b-6ec9-3ad9-954d-3f8ffcc0df82"))) {
return 0;
}
    return ringBufferSlotCount;
  }

  public long rollPeriodMs() {
if(KnobRuntime.check(java.util.UUID.fromString("f04f6b17-d9dd-3a23-839f-3586a508757b"))) {
return 0;
}
    return rollPeriodMs;
  }

  public String archivedWalSuffix() {
if(KnobRuntime.check(java.util.UUID.fromString("1a351562-fdc3-359b-ace0-806603eab743"))) {
return null;
}
    return archivedWalSuffix;
  }

  public String archivedHFileSuffix() {
if(KnobRuntime.check(java.util.UUID.fromString("722c5239-9368-361b-901c-d3e771ae76fa"))) {
return null;
}
    return archivedHFileSuffix;
  }

  public Boolean useMetaCellComparator() {
if(KnobRuntime.check(java.util.UUID.fromString("1c4df35b-850d-3122-9c0e-e9699002e6ca"))) {
return null;
}
    return useMetaCellComparator;
  }
}
