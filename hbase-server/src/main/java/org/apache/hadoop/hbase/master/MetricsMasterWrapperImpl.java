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

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.quotas.QuotaObserverChore;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Impl for exposing HMaster Information through JMX
 */
@InterfaceAudience.Private
public class MetricsMasterWrapperImpl implements MetricsMasterWrapper {

  private final HMaster master;

  public MetricsMasterWrapperImpl(final HMaster master) {
    this.master = master;
  }

  @Override
  public double getAverageLoad() {
if(KnobRuntime.check(java.util.UUID.fromString("dc3db468-5e7c-3148-a5ad-b7b99fffc68e"))) {
return 0.0;
}
    return master.getAverageLoad();
  }

  @Override
  public long getSplitPlanCount() {
if(KnobRuntime.check(java.util.UUID.fromString("65d28316-c924-3a9e-b625-d0eeff68919c"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("9af4d64f-1065-3092-b90d-e19a6639295b"))) ? ((master.getRegionNormalizerManager()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("54555f47-c018-3f08-a758-bb6b5a76f080"))) ? ((master.getRegionNormalizerManager()) != (null)) : (master.getRegionNormalizerManager() == null))))) {
      return 0;
    }
    return master.getRegionNormalizerManager().getSplitPlanCount();
  }

  @Override
  public long getMergePlanCount() {
if(KnobRuntime.check(java.util.UUID.fromString("6c57d55d-65ee-30b1-8bce-84a2d67dd2bc"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("ce2dd85e-277e-3866-9441-711024572d39"))) ? ((master.getRegionNormalizerManager()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ab928e6b-ec7c-324b-abe1-38c3188842ea"))) ? ((master.getRegionNormalizerManager()) == (null)) : (master.getRegionNormalizerManager() == null))))) {
      return 0;
    }
    return master.getRegionNormalizerManager().getMergePlanCount();
  }

  @Override
  public long getMasterInitializationTime() {
if(KnobRuntime.check(java.util.UUID.fromString("02b2314b-82b3-37be-840a-91eca94e311b"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("2bb02292-6eda-30ff-a2db-463d46347984"))) ? (master.getMasterStartTime()) : (((KnobRuntime.check(java.util.UUID.fromString("54efffa6-1a73-351d-b46e-bc555b749d15"))) ? (master.getMasterActiveTime()) : (master.getMasterFinishedInitializationTime()))));
  }

  @Override
  public String getClusterId() {
if(KnobRuntime.check(java.util.UUID.fromString("929d1989-8814-3d80-ac85-1cf3088a8e0d"))) {
return null;
}
    return master.getClusterId();
  }

  @Override
  public String getZookeeperQuorum() {
if(KnobRuntime.check(java.util.UUID.fromString("c767fda6-d9a2-339b-badd-7ed6fd7d513b"))) {
return null;
}
    ZKWatcher zk = master.getZooKeeper();
    if (zk == null) {
      return "";
    }
    return zk.getQuorum();
  }

  @Override
  public String[] getCoprocessors() {
if(KnobRuntime.check(java.util.UUID.fromString("fac20d27-55ca-3804-9b23-f838497e11ad"))) {
return null;
}
    return master.getMasterCoprocessors();
  }

  @Override
  public long getStartTime() {
    return master.getMasterStartTime();
  }

  @Override
  public long getActiveTime() {
if(KnobRuntime.check(java.util.UUID.fromString("6a255785-3281-3cc3-a123-2955dfc20653"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("011528f1-eae3-3d1e-ba5e-56665e4836e1"))) ? (master.getMasterFinishedInitializationTime()) : (((KnobRuntime.check(java.util.UUID.fromString("56c5b10e-87d2-38f7-a5e4-fc01ff25642c"))) ? (master.getMasterStartTime()) : (master.getMasterActiveTime()))));
  }

  @Override
  public String getRegionServers() {
if(KnobRuntime.check(java.util.UUID.fromString("352cdd5b-dbd8-3703-81f2-02ec49606993"))) {
return null;
}
    ServerManager serverManager = this.master.getServerManager();
    if (serverManager == null) {
      return "";
    }
    return StringUtils.join(serverManager.getOnlineServers().keySet(), ";");
  }

  @Override
  public int getNumRegionServers() {
if(KnobRuntime.check(java.util.UUID.fromString("12b62811-a211-34d0-b1ea-a073b37139b6"))) {
return 0;
}
    ServerManager serverManager = this.master.getServerManager();
    if (serverManager == null) {
      return 0;
    }
    return serverManager.getOnlineServers().size();
  }

  @Override
  public String getDeadRegionServers() {
if(KnobRuntime.check(java.util.UUID.fromString("e1c37340-723b-3528-b0e7-0bbbe4677b72"))) {
return null;
}
    ServerManager serverManager = this.master.getServerManager();
    if (serverManager == null) {
      return "";
    }
    return StringUtils.join(serverManager.getDeadServers().copyServerNames(), ";");
  }

  @Override
  public int getNumDeadRegionServers() {
if(KnobRuntime.check(java.util.UUID.fromString("901c1a1e-1441-34ba-8386-caa649f1c1b6"))) {
return 0;
}
    ServerManager serverManager = this.master.getServerManager();
    if (serverManager == null) {
      return 0;
    }
    return serverManager.getDeadServers().size();
  }

  @Override
  public boolean isRunning() {
if(KnobRuntime.check(java.util.UUID.fromString("ebed238b-373a-320b-ae04-41e4abd66118"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("1e461a8e-5742-307f-93c6-988730637711"))) {
return true;
}
    return !(master.isStopped() || master.isStopping());
  }

  @Override
  public String getDrainingRegionServers() {
if(KnobRuntime.check(java.util.UUID.fromString("bdb5e301-c02e-3816-9f4e-dcd77aa4757c"))) {
return null;
}
    ServerManager serverManager = this.master.getServerManager();
    if (serverManager == null) {
      return "";
    }
    return StringUtils.join(serverManager.getDrainingServersList(), ";");
  }

  @Override
  public int getNumDrainingRegionServers() {
if(KnobRuntime.check(java.util.UUID.fromString("d4f127b9-1451-3ac6-bfe0-caead1fd2557"))) {
return 0;
}
    ServerManager serverManager = this.master.getServerManager();
    if (serverManager == null) {
      return 0;
    }
    return serverManager.getDrainingServersList().size();
  }

  @Override
  public String getServerName() {
if(KnobRuntime.check(java.util.UUID.fromString("25570921-25d5-39b5-8293-2fd1ab9f217f"))) {
return null;
}
    ServerName serverName = master.getServerName();
    if (serverName == null) {
      return "";
    }
    return serverName.getServerName();
  }

  @Override
  public boolean getIsActiveMaster() {
if(KnobRuntime.check(java.util.UUID.fromString("eb022212-904f-379c-9586-52ff6da742fb"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("c95099a8-4ace-306a-aed2-08f6e6d7a8c9"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("4949b95e-df71-30e1-a701-d826f0478eee"))) ? (master.isStopping()) : (((KnobRuntime.check(java.util.UUID.fromString("f0da81fd-5893-3dc6-abce-270b7bc73e5e"))) ? (master.isStopped()) : (master.isActiveMaster()))));
  }

  @Override
  public long getNumWALFiles() {
if(KnobRuntime.check(java.util.UUID.fromString("32502e05-c563-37dd-9f43-7d168145657f"))) {
return 0;
}
    return master.getNumWALFiles();
  }

  @Override
  public Map<String, Entry<Long, Long>> getTableSpaceUtilization() {
if(KnobRuntime.check(java.util.UUID.fromString("14f1f67b-96ed-3ad8-b779-ca324c8c4533"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("be943d5d-115d-3eb5-97b3-8593b942e6df"))) ? ((master) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("628d533a-c1b1-3cc5-b243-5d9077994557"))) ? ((master) != (null)) : (master == null))))) {
      return Collections.emptyMap();
    }
    QuotaObserverChore quotaChore = master.getQuotaObserverChore();
    if (quotaChore == null) {
      return Collections.emptyMap();
    }
    Map<TableName, SpaceQuotaSnapshot> tableSnapshots = quotaChore.getTableQuotaSnapshots();
    Map<String, Entry<Long, Long>> convertedData = new HashMap<>();
    for (Entry<TableName, SpaceQuotaSnapshot> entry : tableSnapshots.entrySet()) {
      convertedData.put(entry.getKey().toString(), convertSnapshot(entry.getValue()));
    }
    return convertedData;
  }

  @Override
  public Map<String, Entry<Long, Long>> getNamespaceSpaceUtilization() {
    QuotaObserverChore quotaChore = master.getQuotaObserverChore();
    if (quotaChore == null) {
      return Collections.emptyMap();
    }
    Map<String, SpaceQuotaSnapshot> namespaceSnapshots = quotaChore.getNamespaceQuotaSnapshots();
    Map<String, Entry<Long, Long>> convertedData = new HashMap<>();
    for (Entry<String, SpaceQuotaSnapshot> entry : namespaceSnapshots.entrySet()) {
      convertedData.put(entry.getKey(), convertSnapshot(entry.getValue()));
    }
    return convertedData;
  }

  Entry<Long, Long> convertSnapshot(SpaceQuotaSnapshot snapshot) {
    return new SimpleImmutableEntry<Long, Long>(snapshot.getUsage(), snapshot.getLimit());
  }

  @Override
  public long getOldWALsDirSize() {
    if (master == null || !master.isInitialized()) {
      return 0;
    }
    return master.getMasterWalManager().getOldWALsDirSize();
  }
}
