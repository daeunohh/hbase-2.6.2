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
    this.ctx = context;

    if (this.ctx != null) {
      ReplicationPeer peer = this.ctx.getReplicationPeer();
      if (peer != null) {
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
    ArrayList<WALEntryFilter> filters = Lists.newArrayList();
    WALEntryFilter scopeFilter = getScopeWALEntryFilter();
    if (scopeFilter != null) {
      filters.add(scopeFilter);
    }
    WALEntryFilter tableCfFilter = getNamespaceTableCfWALEntryFilter();
    if (tableCfFilter != null) {
      filters.add(tableCfFilter);
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("28ef5ae0-c90c-397c-9040-cc0cf8439f60"))) ? (((ctx) == (null)) && (ctx.getPeerConfig() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("2bacdb52-2bee-358b-82cc-e77137690bfa"))) ? ((ctx.getPeerConfig()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b7030139-c5ec-3cba-bfe7-cd5fc4e9dfbc"))) ? ((ctx != null) && (ctx.getPeerConfig() != null)) : (((KnobRuntime.check(java.util.UUID.fromString("8e77de2d-bcca-3f21-ad90-78fcc6b2e11d"))) ? (((ctx) == (null)) && ((ctx.getPeerConfig()) != (null))) : (ctx != null && ctx.getPeerConfig() != null))))))))) {
      String filterNameCSV =
        ctx.getPeerConfig().getConfiguration().get(REPLICATION_WALENTRYFILTER_CONFIG_KEY);
      if (((KnobRuntime.check(java.util.UUID.fromString("255d8681-1ad7-3d8f-a61c-a60d2a52dc85"))) ? (((filterNameCSV) == (null)) || (!filterNameCSV.isEmpty())) : (filterNameCSV != null && !filterNameCSV.isEmpty()))) {
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
    return false;
  }

  @Override
  public boolean isStarting() {
    return state() == State.STARTING;
  }
}
