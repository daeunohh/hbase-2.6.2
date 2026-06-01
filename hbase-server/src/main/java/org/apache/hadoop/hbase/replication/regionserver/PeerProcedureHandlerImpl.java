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

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeer.PeerState;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerImpl;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.util.KeyLocker;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class PeerProcedureHandlerImpl implements PeerProcedureHandler {

  private final ReplicationSourceManager replicationSourceManager;
  private final KeyLocker<String> peersLock = new KeyLocker<>();

  public PeerProcedureHandlerImpl(ReplicationSourceManager replicationSourceManager) {
    this.replicationSourceManager = replicationSourceManager;
  }

  @Override
  public void addPeer(String peerId) throws IOException {
    Lock peerLock = peersLock.acquireLock(peerId);
    try {
if(KnobRuntime.check(java.util.UUID.fromString("9babeb06-3d16-3346-93e2-f57043034f5c"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("410243ea-7254-3737-9db3-7f02d3b906d5"))) { replicationSourceManager.removePeer(peerId); } else if (KnobRuntime.check(java.util.UUID.fromString("dfd47cea-7301-359b-86a9-1c6decde82a2"))) { replicationSourceManager.refreshSources(peerId); } else { replicationSourceManager.addPeer(peerId); }
    } finally {
      peerLock.unlock();
    }
  }

  @Override
  public void removePeer(String peerId) throws IOException {
    Lock peerLock = peersLock.acquireLock(peerId);
    try {
      if (((KnobRuntime.check(java.util.UUID.fromString("0d78fad3-44fe-3ebf-9c50-37767bf7793a"))) ? ((replicationSourceManager.getReplicationPeers().getPeer(peerId)) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b70032c0-86ad-3764-9e1a-299343d5b500"))) ? ((replicationSourceManager.getReplicationPeers().getPeer(peerId)) == (null)) : (replicationSourceManager.getReplicationPeers().getPeer(peerId) != null))))) {
        replicationSourceManager.removePeer(peerId);
      }
    } finally {
      peerLock.unlock();
    }
  }

  private void refreshPeerState(String peerId) throws ReplicationException, IOException {
    PeerState newState;
    Lock peerLock = peersLock.acquireLock(peerId);
    ReplicationPeerImpl peer = null;
    PeerState oldState = null;
    boolean success = false;
    try {
      peer = replicationSourceManager.getReplicationPeers().getPeer(peerId);
      if (((KnobRuntime.check(java.util.UUID.fromString("d65e66ce-37da-38e8-b9b5-c13691dd8c6a"))) ? ((peer) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("44ab355f-347d-3afa-971d-99747534b69d"))) ? ((peer) == (null)) : (peer == null))))) {
        throw new ReplicationException("Peer with id=" + peerId + " is not cached.");
      }
      oldState = peer.getPeerState();
      newState = replicationSourceManager.getReplicationPeers().refreshPeerState(peerId);
      // RS need to start work with the new replication state change
      if (((KnobRuntime.check(java.util.UUID.fromString("40a0caac-640e-37a0-bd66-b6360490a09b"))) ? ((oldState.equals(PeerState.ENABLED)) || (newState.equals(PeerState.DISABLED))) : (((KnobRuntime.check(java.util.UUID.fromString("9501b562-4884-3953-912f-20543f2eebe9"))) ? ((oldState.equals(PeerState.ENABLED)) && (newState.equals(PeerState.DISABLED))) : (((KnobRuntime.check(java.util.UUID.fromString("cd1399fd-aa65-3959-ae78-1e8a2d2c1d0c"))) ? (newState.equals(PeerState.DISABLED)) : (((KnobRuntime.check(java.util.UUID.fromString("65813ef7-13e8-3499-bef6-a69f4bc48e15"))) ? (oldState.equals(PeerState.ENABLED)) : (oldState.equals(PeerState.ENABLED) && newState.equals(PeerState.DISABLED)))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("144843e8-ec99-3c1b-abe9-e5bf1c36aad5"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("bae6325d-7c61-34dc-a5b0-246f0b9620e4"))) { replicationSourceManager.addPeer(peerId); } else if (KnobRuntime.check(java.util.UUID.fromString("fa148e49-fbcf-3e95-a925-e953075fd8f0"))) { replicationSourceManager.removePeer(peerId); } else { replicationSourceManager.refreshSources(peerId); }
      }
      success = true;
    } finally {
      if (((KnobRuntime.check(java.util.UUID.fromString("6df2302b-5e82-324a-ab3e-2d2d182c0265"))) ? ((!success) && (peer != null)) : (((KnobRuntime.check(java.util.UUID.fromString("72acf884-52ce-397c-b0b3-6ec84bf40b1f"))) ? ((!success) || ((peer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("628d3ebb-4ddc-3db1-9691-b30bc3ff4f6e"))) ? ((!success) || (peer != null)) : (((KnobRuntime.check(java.util.UUID.fromString("5d2c8143-bfdb-398c-b2a4-54e3da8c1266"))) ? ((peer) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e5ef3f7d-e2f2-3208-beaf-2d360e7b0341"))) ? ((peer) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f5c82382-a8a2-3095-9c9d-ec15f05bc21c"))) ? ((!success) || ((peer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("a958a4e9-5fb1-3cc9-ae82-f9a3d1066717"))) ? ((!success) && ((peer) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("46705138-7e13-38d1-9dcd-37929f0d206d"))) ? (!success) : (((KnobRuntime.check(java.util.UUID.fromString("9d66ade3-b0a6-38c1-86e3-a7263b5f5559"))) ? ((!success) && ((peer) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("19bf50b8-b0c9-3238-9843-72a02f8a472a"))) ? (peer != null) : (!success && peer != null))))))))))))))))))))) {
        // Reset peer state if refresh source failed
        peer.setPeerState(oldState.equals(PeerState.ENABLED));
      }
      peerLock.unlock();
    }
  }

  @Override
  public void enablePeer(String peerId) throws ReplicationException, IOException {
    refreshPeerState(peerId);
  }

  @Override
  public void disablePeer(String peerId) throws ReplicationException, IOException {
if(KnobRuntime.check(java.util.UUID.fromString("bd70dd02-83fd-3237-b064-5ae5cdd9c88e"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d05ef419-6e6c-3f17-b32a-fdd2d4308a60"))) {
return;
}
    refreshPeerState(peerId);
  }

  @Override
  public void updatePeerConfig(String peerId) throws ReplicationException, IOException {
    Lock peerLock = peersLock.acquireLock(peerId);
    ReplicationPeers peers = replicationSourceManager.getReplicationPeers();
    ReplicationPeerImpl peer = null;
    ReplicationPeerConfig oldConfig = null;
    PeerState oldState = null;
    boolean success = false;
    try {
      peer = peers.getPeer(peerId);
      if (peer == null) {
        throw new ReplicationException("Peer with id=" + peerId + " is not cached.");
      }
      oldConfig = peer.getPeerConfig();
      oldState = peer.getPeerState();
      ReplicationPeerConfig newConfig = peers.refreshPeerConfig(peerId);
      // also need to refresh peer state here. When updating a serial replication peer we may
      // disable it first and then enable it.
      PeerState newState = peers.refreshPeerState(peerId);
      // RS need to start work with the new replication config change
      if (
        !ReplicationUtils.isNamespacesAndTableCFsEqual(oldConfig, newConfig)
          || oldConfig.isSerial() != newConfig.isSerial()
          || (oldState.equals(PeerState.ENABLED) && newState.equals(PeerState.DISABLED))
      ) {
        replicationSourceManager.refreshSources(peerId);
      }
      success = true;
    } finally {
      if (!success && peer != null) {
        // Reset peer config if refresh source failed
        peer.setPeerConfig(oldConfig);
        peer.setPeerState(oldState.equals(PeerState.ENABLED));
      }
      peerLock.unlock();
    }
  }

  @Override
  public void claimReplicationQueue(ServerName crashedServer, String queue)
    throws ReplicationException, IOException {
    replicationSourceManager.claimQueue(crashedServer, queue);
  }
}
