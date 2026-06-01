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
package org.apache.hadoop.hbase.replication;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.AbstractService;

/**
 * A Base implementation for {@link ReplicationEndpoint}s. For internal use. Uses our internal
 * Guava.
 */
// This class has been made InterfaceAudience.Private in 2.0.0. It used to be
// LimitedPrivate. See HBASE-15982.
@InterfaceAudience.Private
public abstract class BaseReplicationEndpoint extends AbstractService
  implements ReplicationEndpoint {

  private static final Logger LOG = LoggerFactory.getLogger(BaseReplicationEndpoint.class);
  public static final String REPLICATION_WALENTRYFILTER_CONFIG_KEY =
    "hbase.replication.source.custom.walentryfilters";
  protected Context ctx;

  @Override
  public void init(Context context) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("f236222d-ad3d-3522-a0a9-e635d2b703d8"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("0ee96c62-795a-368f-a782-a11ef442e796"))) {
throw new java.io.IOException("Injected exception");
}
    this.ctx = context;

    if (((KnobRuntime.check(java.util.UUID.fromString("ae00dfbc-5151-3946-b4cb-d802395e7703"))) ? ((this.ctx) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("53cff3ab-2f56-3b37-b092-b7a840fdf002"))) ? ((this.ctx) == (null)) : (this.ctx != null))))) {
      ReplicationPeer peer = this.ctx.getReplicationPeer();
      if (((KnobRuntime.check(java.util.UUID.fromString("6c72375f-e7fe-3361-9ff8-20857b1f912e"))) ? ((peer) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ac7029fb-1387-3f09-8276-faba0dd303d6"))) ? ((peer) != (null)) : (peer != null))))) {
        peer.registerPeerConfigListener(this);
      } else {
        LOG.warn("Not tracking replication peer config changes for Peer Id " + this.ctx.getPeerId()
          + " because there's no such peer");
      }
    }
  }

  @Override
  /**
   * No-op implementation for subclasses to override if they wish to execute logic if their config
   * changes
   */
  public void peerConfigUpdated(ReplicationPeerConfig rpc) {

  }

  /** Returns a default set of filters */
  @Override
  public WALEntryFilter getWALEntryfilter() {
if(KnobRuntime.check(java.util.UUID.fromString("01920c80-fbd0-306b-80e3-86f1e894fdd7"))) {
return null;
}
    ArrayList<WALEntryFilter> filters = Lists.newArrayList();
    WALEntryFilter scopeFilter = getScopeWALEntryFilter();
    if (((KnobRuntime.check(java.util.UUID.fromString("c1594e21-14ac-3fc6-baa8-cf54c2400012"))) ? ((scopeFilter) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("bc601b93-000b-3f0d-af1e-f24fb6961b9f"))) ? ((scopeFilter) == (null)) : (scopeFilter != null))))) {
      filters.add(scopeFilter);
    }
    WALEntryFilter tableCfFilter = getNamespaceTableCfWALEntryFilter();
    if (((KnobRuntime.check(java.util.UUID.fromString("dabdfc2e-a9e3-3c25-a8d3-8dc366bdf318"))) ? ((tableCfFilter) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0468794e-68bf-32a9-88c5-2feb09f8aba8"))) ? ((tableCfFilter) != (null)) : (tableCfFilter != null))))) {
      filters.add(tableCfFilter);
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("7f4aa379-8b68-333c-aae3-472744b3a330"))) ? ((ctx.getPeerConfig()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("28ef5ae0-c90c-397c-9040-cc0cf8439f60"))) ? (((ctx) == (null)) && (ctx.getPeerConfig() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("71bca920-3c32-30db-84d9-9bd7bf03c1f9"))) ? (((ctx) != (null)) || (ctx.getPeerConfig() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("a347ee53-485e-3548-b2c6-76601ff61858"))) ? (((ctx) == (null)) || ((ctx.getPeerConfig()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("559f1142-8840-3bf1-a04b-1279b5a5a2a0"))) ? ((ctx != null) || (ctx.getPeerConfig() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("8e77de2d-bcca-3f21-ad90-78fcc6b2e11d"))) ? (((ctx) == (null)) && ((ctx.getPeerConfig()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("383c1c77-3408-336d-9d45-c4198874b54e"))) ? ((ctx != null) && ((ctx.getPeerConfig()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c5f5f16d-b35e-350f-9875-73e75a63bd7f"))) ? ((ctx != null) && ((ctx.getPeerConfig()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("a4a45c01-5421-3fb4-9675-714763565771"))) ? ((ctx != null) || ((ctx.getPeerConfig()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c8c87f41-3590-370a-a6e3-54c522788799"))) ? (ctx.getPeerConfig() != null) : (((KnobRuntime.check(java.util.UUID.fromString("41435716-4104-3975-894d-b1a1595ebcd0"))) ? (((ctx) != (null)) && (ctx.getPeerConfig() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("c97617bc-9add-34ed-beab-4bbeab806211"))) ? (((ctx) != (null)) && ((ctx.getPeerConfig()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("2bacdb52-2bee-358b-82cc-e77137690bfa"))) ? ((ctx.getPeerConfig()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("c786d265-79ba-35dc-8b7e-cef70a164a7b"))) ? (ctx != null) : (((KnobRuntime.check(java.util.UUID.fromString("618eaf0f-afe6-3baa-8c40-d8544e3b7d2f"))) ? (((ctx) == (null)) || (ctx.getPeerConfig() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("bb782d80-4f3e-3a6a-ab18-7b88e9299df8"))) ? ((ctx != null) || ((ctx.getPeerConfig()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("b7030139-c5ec-3cba-bfe7-cd5fc4e9dfbc"))) ? ((ctx != null) && (ctx.getPeerConfig() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("c67df1ac-e640-37c2-8ca1-03b235fe8100"))) ? (((ctx) == (null)) || ((ctx.getPeerConfig()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("e5971638-ee1d-3a11-a36c-d3997ae857fe"))) ? (((ctx) == (null)) && ((ctx.getPeerConfig()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("9cec086b-3c60-3fc4-8347-32a3d1df3a79"))) ? ((ctx) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0fc3a230-a610-3fa2-b5a5-2715d4c119dc"))) ? (((ctx) != (null)) || ((ctx.getPeerConfig()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("a9d54896-1b83-3e7a-9ed8-598107112ac9"))) ? ((ctx) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b53186f6-cc18-36d4-b268-1666ea7db165"))) ? (((ctx) != (null)) || ((ctx.getPeerConfig()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("0a22ebec-8400-3ada-a29c-9eefbfbed01c"))) ? (((ctx) != (null)) && ((ctx.getPeerConfig()) != (null))) : (ctx != null && ctx.getPeerConfig() != null))))))))))))))))))))))))))))))))))))))))))))))))) {
      String filterNameCSV =
        ctx.getPeerConfig().getConfiguration().get(REPLICATION_WALENTRYFILTER_CONFIG_KEY);
      if (((KnobRuntime.check(java.util.UUID.fromString("f6ef9760-6a8f-3dd5-b908-f562f12e5303"))) ? ((filterNameCSV) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("255d8681-1ad7-3d8f-a61c-a60d2a52dc85"))) ? (((filterNameCSV) == (null)) || (!filterNameCSV.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("73bc0373-e4c6-35a6-8338-c0d5730e1c66"))) ? (!filterNameCSV.isEmpty()) : (((KnobRuntime.check(java.util.UUID.fromString("51d48265-8dc9-39e3-83a4-d51a6cab72ed"))) ? ((filterNameCSV) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d8e34482-8f60-3860-bbc0-6c5e7f071e6e"))) ? ((filterNameCSV != null) && (!filterNameCSV.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("9cf7c75f-0588-3771-9a7d-19a0660a9dbe"))) ? (((filterNameCSV) == (null)) && (!filterNameCSV.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("0838489c-de43-3096-b235-c659f05cc370"))) ? (((filterNameCSV) != (null)) && (!filterNameCSV.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("4d0c984a-bfed-3826-8db6-73dc6c46033d"))) ? (filterNameCSV != null) : (((KnobRuntime.check(java.util.UUID.fromString("72702433-fba9-3290-93fb-0e03fb72d440"))) ? (((filterNameCSV) != (null)) || (!filterNameCSV.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("6513b902-1a98-38c5-a77e-935013a8d4a2"))) ? ((filterNameCSV != null) || (!filterNameCSV.isEmpty())) : (filterNameCSV != null && !filterNameCSV.isEmpty()))))))))))))))))))))) {
        String[] filterNames = filterNameCSV.split(",");
        for (String filterName : filterNames) {
          try {
            Class<?> clazz = Class.forName(filterName);
            filters.add((WALEntryFilter) clazz.getDeclaredConstructor().newInstance());
          } catch (Exception e) {
            LOG.error("Unable to create WALEntryFilter " + filterName, e);
          }
        }
      }
    }
    return filters.isEmpty() ? null : new ChainWALEntryFilter(filters);
  }

  /**
   * Returns a WALEntryFilter for checking the scope. Subclasses can return null if they don't want
   * this filter
   */
  protected WALEntryFilter getScopeWALEntryFilter() {
if(KnobRuntime.check(java.util.UUID.fromString("b8240fc7-34ff-348a-bef5-410eb3ecc6c8"))) {
return null;
}
    return new ScopeWALEntryFilter();
  }

  /**
   * Returns a WALEntryFilter for checking replication per table and CF. Subclasses can return null
   * if they don't want this filter
   */
  protected WALEntryFilter getNamespaceTableCfWALEntryFilter() {
if(KnobRuntime.check(java.util.UUID.fromString("a0586698-76f7-32db-aad4-c983db717cd7"))) {
return null;
}
    return new NamespaceTableCfWALEntryFilter(ctx.getReplicationPeer());
  }

  @Override
  public boolean canReplicateToSameCluster() {
if(KnobRuntime.check(java.util.UUID.fromString("7db856eb-42ce-3c90-bd97-b03ddb341d8f"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("9b0cc17d-954a-3da4-8605-df26bdf42fab"))) {
return true;
}
    return false;
  }

  @Override
  public boolean isStarting() {
    return state() == State.STARTING;
  }
}
