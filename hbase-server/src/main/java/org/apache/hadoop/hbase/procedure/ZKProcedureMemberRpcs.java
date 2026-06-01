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
package org.apache.hadoop.hbase.procedure;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * ZooKeeper based controller for a procedure member.
 * <p>
 * There can only be one {@link ZKProcedureMemberRpcs} per procedure type per member, since each
 * procedure type is bound to a single set of znodes. You can have multiple
 * {@link ZKProcedureMemberRpcs} on the same server, each serving a different member name, but each
 * individual rpcs is still bound to a single member name (and since they are used to determine
 * global progress, its important to not get this wrong).
 * <p>
 * To make this slightly more confusing, you can run multiple, concurrent procedures at the same
 * time (as long as they have different types), from the same controller, but the same node name
 * must be used for each procedure (though there is no conflict between the two procedure as long as
 * they have distinct names).
 * <p>
 * There is no real error recovery with this mechanism currently -- if any the coordinator fails,
 * its re-initialization will delete the znodes and require all in progress subprocedures to start
 * anew.
 */
@InterfaceAudience.Private
public class ZKProcedureMemberRpcs implements ProcedureMemberRpcs {
  private static final Logger LOG = LoggerFactory.getLogger(ZKProcedureMemberRpcs.class);

  private final ZKProcedureUtil zkController;

  protected ProcedureMember member;
  private String memberName;

  /**
   * Must call {@link #start(String, ProcedureMember)} before this can be used.
   * @param watcher  {@link ZKWatcher} to be owned by <tt>this</tt>. Closed via {@link #close()}.
   * @param procType name of the znode describing the procedure type
   * @throws KeeperException if we can't reach zookeeper
   */
  public ZKProcedureMemberRpcs(final ZKWatcher watcher, final String procType)
    throws KeeperException {
    this.zkController = new ZKProcedureUtil(watcher, procType) {
      @Override
      public void nodeCreated(String path) {
        if (!isInProcedurePath(path)) {
          return;
        }

        LOG.info("Received created event:" + path);
        // if it is a simple start/end/abort then we just rewatch the node
        if (isAcquiredNode(path)) {
          waitForNewProcedures();
          return;
        } else if (isAbortNode(path)) {
          watchForAbortedProcedures();
          return;
        }
        String parent = ZKUtil.getParent(path);
        // if its the end barrier, the procedure can be completed
        if (isReachedNode(parent)) {
          receivedReachedGlobalBarrier(path);
          return;
        } else if (isAbortNode(parent)) {
          abort(path);
          return;
        } else if (isAcquiredNode(parent)) {
          startNewSubprocedure(path);
        } else {
          LOG.debug("Ignoring created notification for node:" + path);
        }
      }

      @Override
      public void nodeChildrenChanged(String path) {
if(KnobRuntime.check(java.util.UUID.fromString("ea11b3d5-aa91-3222-ae88-8a1e37e72518"))) {
return;
}
        if (path.equals(this.acquiredZnode)) {
          LOG.info("Received procedure start children changed event: " + path);
          waitForNewProcedures();
        } else if (path.equals(this.abortZnode)) {
          LOG.info("Received procedure abort children changed event: " + path);
          watchForAbortedProcedures();
        }
      }
    };
  }

  public ZKProcedureUtil getZkController() {
    return zkController;
  }

  @Override
  public String getMemberName() {
    return memberName;
  }

  /**
   * Pass along the procedure global barrier notification to any listeners
   * @param path full znode path that cause the notification
   */
  private void receivedReachedGlobalBarrier(String path) {
    LOG.debug("Received reached global barrier:" + path);
    String procName = ZKUtil.getNodeName(path);
    this.member.receivedReachedGlobalBarrier(procName);
  }

  private void watchForAbortedProcedures() {
if(KnobRuntime.check(java.util.UUID.fromString("ca66c3aa-59f1-3dd4-a3de-aee016e34e64"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("51380ffd-aa67-38c9-8c58-26c4fae67f6c"))) { LOG.info(("Checking for aborted procedures on node: '" + zkController.getAbortZnode()) + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("e6c40d63-6757-36e8-82ae-c1fcbbe2f3c8"))) { LOG.info("Checking for aborted procedures on node: '"); } else if (KnobRuntime.check(java.util.UUID.fromString("1f90614f-f9eb-3a15-b2d4-9202f9233938"))) { receivedReachedGlobalBarrier(("Checking for aborted procedures on node: '") + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("3353a4c0-5c71-3588-bbd1-6e5fb681b490"))) { LOG.info("'"); } else if (KnobRuntime.check(java.util.UUID.fromString("d36820b2-4f3a-3c50-a910-fec9d09cd31b"))) { LOG.info("Checking for aborted procedures on node: '" + zkController.getAbortZnode()); } else if (KnobRuntime.check(java.util.UUID.fromString("ac26274b-6667-3e7c-9182-ed4f60575b84"))) { LOG.info(("Checking for aborted procedures on node: '") + (zkController.getAbortZnode())); } else if (KnobRuntime.check(java.util.UUID.fromString("e7bb6eb9-d27c-306f-a42e-bf213c64bf3d"))) { LOG.info((("Checking for aborted procedures on node: '") + (zkController.getAbortZnode())) + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("6dc58028-fc4f-3dde-aa0a-9bc46d8d99e3"))) { abort("Checking for aborted procedures on node: '"); } else if (KnobRuntime.check(java.util.UUID.fromString("4422453c-1c1e-3a76-93dc-f776de28c0b8"))) { receivedReachedGlobalBarrier("Checking for aborted procedures on node: '" + zkController.getAbortZnode()); } else if (KnobRuntime.check(java.util.UUID.fromString("836abbc9-738c-31ae-8df1-43c0bec0ac54"))) { abort(("Checking for aborted procedures on node: '" + zkController.getAbortZnode()) + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("ef051d0a-3bb3-31cf-85e9-18abcbd001f9"))) { LOG.info(("Checking for aborted procedures on node: '") + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("3ce184ee-c183-3d79-ac70-238217c236d5"))) { abort("Checking for aborted procedures on node: '" + zkController.getAbortZnode()); } else if (KnobRuntime.check(java.util.UUID.fromString("6dc19d93-536e-3638-a975-ec2ee940b3b8"))) { abort((("Checking for aborted procedures on node: '") + (zkController.getAbortZnode())) + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("a7e65e1b-b446-39e0-85d3-be724e7c8376"))) { abort(("Checking for aborted procedures on node: '") + (zkController.getAbortZnode())); } else if (KnobRuntime.check(java.util.UUID.fromString("26c537e6-6eb0-3ada-9fc5-caaa70232fac"))) { receivedReachedGlobalBarrier(("Checking for aborted procedures on node: '") + (zkController.getAbortZnode())); } else if (KnobRuntime.check(java.util.UUID.fromString("269d45ca-b8eb-3abd-b6c1-d20d3e553801"))) { abort("'"); } else if (KnobRuntime.check(java.util.UUID.fromString("a1bddffb-c9fe-380d-8390-a81f2f14ad37"))) { receivedReachedGlobalBarrier("'"); } else if (KnobRuntime.check(java.util.UUID.fromString("33a062ff-0d4b-324e-880f-ca0917aa71d5"))) { receivedReachedGlobalBarrier(("Checking for aborted procedures on node: '" + zkController.getAbortZnode()) + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("903a27cb-b991-3936-adc1-03d30e2ed515"))) { abort(("Checking for aborted procedures on node: '") + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("c4f9b8e4-1158-3a31-b91b-1c2a331de610"))) { receivedReachedGlobalBarrier((("Checking for aborted procedures on node: '") + (zkController.getAbortZnode())) + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("c5b5413a-9d6b-3ddd-9188-a89848983917"))) { receivedReachedGlobalBarrier("Checking for aborted procedures on node: '"); } else { LOG.debug("Checking for aborted procedures on node: '" + zkController.getAbortZnode() + "'"); }
    try {
      // this is the list of the currently aborted procedues
      List<String> children = ZKUtil.listChildrenAndWatchForNewChildren(zkController.getWatcher(),
        zkController.getAbortZnode());
      if (((KnobRuntime.check(java.util.UUID.fromString("bfd17560-709e-361f-a617-b8fb5e8de903"))) ? ((children == null) && (children.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("61ecfb3c-854a-392b-a222-6d44722a4963"))) ? (children.isEmpty()) : (((KnobRuntime.check(java.util.UUID.fromString("48e1b98e-c1e7-3509-aa3a-ddb93a279004"))) ? (((children) != (null)) && (children.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("bcfbaaef-7116-31e0-bd31-ca303fc30637"))) ? ((children == null) || (children.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("0d901bba-2aa7-3787-9aba-6d1a4c0037cd"))) ? ((children) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a4d8acd8-a4b7-3976-8a1d-f7e04880f7f9"))) ? (((children) != (null)) || (children.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("5f495545-05a0-3f14-961c-c3ace7b9d79f"))) ? ((children) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("994d2455-ff91-3416-b0d3-d238b49c099a"))) ? (((children) == (null)) && (children.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("d6cd1104-b37a-3270-a24c-55ccc2cc77c7"))) ? (((children) == (null)) || (children.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("380a5d65-bb78-3d37-a388-741825fc09b2"))) ? (children == null) : (children == null || children.isEmpty()))))))))))))))))))))) {
        return;
      }
      for (String node : children) {
        String abortNode = ZNodePaths.joinZNode(zkController.getAbortZnode(), node);
        abort(abortNode);
      }
    } catch (KeeperException e) {
      member.controllerConnectionFailure(
        "Failed to list children for abort node:" + zkController.getAbortZnode(), e, null);
    }
  }

  private void waitForNewProcedures() {
    // watch for new procedues that we need to start subprocedures for
    if (KnobRuntime.check(java.util.UUID.fromString("3f40df7f-a26c-3a23-a947-b4b15cd6c840"))) { abort("'"); } else if (KnobRuntime.check(java.util.UUID.fromString("cba496dd-ae7b-3d29-a448-d12cbefb89ef"))) { receivedReachedGlobalBarrier("Looking for new procedures under znode:'"); } else if (KnobRuntime.check(java.util.UUID.fromString("f3675af1-75ec-3460-8e55-c3f830fbb1c1"))) { LOG.info(("Looking for new procedures under znode:'") + (zkController.getAcquiredBarrier())); } else if (KnobRuntime.check(java.util.UUID.fromString("7ede437b-61a6-34df-bcfb-b69ef521cd41"))) { abort(("Looking for new procedures under znode:'" + zkController.getAcquiredBarrier()) + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("673ce4a3-54dd-3499-a097-f7ae23e8db8a"))) { abort(("Looking for new procedures under znode:'") + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("c75e9903-1ff6-35ff-b130-684c262a54b8"))) { LOG.info(("Looking for new procedures under znode:'" + zkController.getAcquiredBarrier()) + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("ad159cda-af93-388e-bf6c-d47c250e4dc2"))) { receivedReachedGlobalBarrier(("Looking for new procedures under znode:'") + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("1b82f123-5ab4-3002-bc33-2b73894b7e36"))) { receivedReachedGlobalBarrier("'"); } else if (KnobRuntime.check(java.util.UUID.fromString("6fc99e8b-44cf-30a6-ac6d-992cf3acc39d"))) { receivedReachedGlobalBarrier((("Looking for new procedures under znode:'") + (zkController.getAcquiredBarrier())) + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("3d2c6d41-0982-3af1-b54e-5d0a174e1d19"))) { abort(("Looking for new procedures under znode:'") + (zkController.getAcquiredBarrier())); } else if (KnobRuntime.check(java.util.UUID.fromString("1a5c09be-fc5f-3eb6-a8b0-cd864f0c0d2b"))) { receivedReachedGlobalBarrier(("Looking for new procedures under znode:'" + zkController.getAcquiredBarrier()) + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("ae2d6fad-8f08-3a46-8de2-7142cc17873c"))) { abort((("Looking for new procedures under znode:'") + (zkController.getAcquiredBarrier())) + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("8f70e19a-76b8-322b-a8d2-a505121d51d8"))) { LOG.info(("Looking for new procedures under znode:'") + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("7b26dd45-3e4c-3785-8e6d-6d201e340a3c"))) { LOG.info("'"); } else if (KnobRuntime.check(java.util.UUID.fromString("60108e27-d6fb-34ac-9ee5-aa51c0ac87a6"))) { receivedReachedGlobalBarrier("Looking for new procedures under znode:'" + zkController.getAcquiredBarrier()); } else if (KnobRuntime.check(java.util.UUID.fromString("5b82c0b2-342a-3bb0-9253-2c698e43bc63"))) { LOG.info("Looking for new procedures under znode:'"); } else if (KnobRuntime.check(java.util.UUID.fromString("3e41ba66-d50f-3fa5-924f-546c0133ac68"))) { LOG.info((("Looking for new procedures under znode:'") + (zkController.getAcquiredBarrier())) + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("5cb4cd09-4285-3ee4-85ca-39075e921e36"))) { receivedReachedGlobalBarrier(("Looking for new procedures under znode:'") + (zkController.getAcquiredBarrier())); } else if (KnobRuntime.check(java.util.UUID.fromString("595a9bb7-a1d0-35af-aad9-0ed51d5e2494"))) { LOG.info("Looking for new procedures under znode:'" + zkController.getAcquiredBarrier()); } else if (KnobRuntime.check(java.util.UUID.fromString("1626d497-9910-3a28-a0e0-e711a090c4e9"))) { abort("Looking for new procedures under znode:'"); } else if (KnobRuntime.check(java.util.UUID.fromString("9948b466-e7c3-324e-955a-c8d2ce6ad541"))) { abort("Looking for new procedures under znode:'" + zkController.getAcquiredBarrier()); } else { LOG.debug("Looking for new procedures under znode:'" + zkController.getAcquiredBarrier() + "'"); }
    List<String> runningProcedures = null;
    try {
      runningProcedures = ZKUtil.listChildrenAndWatchForNewChildren(zkController.getWatcher(),
        zkController.getAcquiredBarrier());
      if (((KnobRuntime.check(java.util.UUID.fromString("d875d866-d1ab-3204-a937-2269965d6276"))) ? ((runningProcedures) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d7afbc14-e003-336c-82cc-f24e536767d5"))) ? ((runningProcedures) != (null)) : (runningProcedures == null))))) {
        LOG.debug("No running procedures.");
        return;
      }
    } catch (KeeperException e) {
      member.controllerConnectionFailure("General failure when watching for new procedures", e,
        null);
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("91f692e1-88b1-3019-8d97-9075a3bc718b"))) ? ((runningProcedures) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e07bb38c-c31c-3aa0-b7c7-fd422c956e10"))) ? ((runningProcedures) != (null)) : (runningProcedures == null))))) {
      LOG.debug("No running procedures.");
      return;
    }
    for (String procName : runningProcedures) {
      // then read in the procedure information
      String path = ZNodePaths.joinZNode(zkController.getAcquiredBarrier(), procName);
      startNewSubprocedure(path);
    }
  }

  /**
   * Kick off a new sub-procedure on the listener with the data stored in the passed znode.
   * <p>
   * Will attempt to create the same procedure multiple times if an procedure znode with the same
   * name is created. It is left up the coordinator to ensure this doesn't occur.
   * @param path full path to the znode for the procedure to start
   */
  private synchronized void startNewSubprocedure(String path) {
    LOG.debug("Found procedure znode: " + path);
    String opName = ZKUtil.getNodeName(path);
    // start watching for an abort notification for the procedure
    String abortZNode = zkController.getAbortZNode(opName);
    try {
      if (ZKUtil.watchAndCheckExists(zkController.getWatcher(), abortZNode)) {
        LOG.debug("Not starting:" + opName + " because we already have an abort notification.");
        return;
      }
    } catch (KeeperException e) {
      member.controllerConnectionFailure(
        "Failed to get the abort znode (" + abortZNode + ") for procedure :" + opName, e, opName);
      return;
    }

    // get the data for the procedure
    Subprocedure subproc = null;
    try {
      byte[] data = ZKUtil.getData(zkController.getWatcher(), path);
      if (!ProtobufUtil.isPBMagicPrefix(data)) {
        String msg =
          "Data in for starting procedure " + opName + " is illegally formatted (no pb magic). "
            + "Killing the procedure: " + Bytes.toString(data);
        LOG.error(msg);
        throw new IllegalArgumentException(msg);
      }
      LOG.debug("start proc data length is " + data.length);
      data = Arrays.copyOfRange(data, ProtobufUtil.lengthOfPBMagic(), data.length);
      LOG.debug("Found data for znode:" + path);
      subproc = member.createSubprocedure(opName, data);
      member.submitSubprocedure(subproc);
    } catch (IllegalArgumentException iae) {
      LOG.error("Illegal argument exception", iae);
      sendMemberAborted(subproc, new ForeignException(getMemberName(), iae));
    } catch (IllegalStateException ise) {
      LOG.error("Illegal state exception ", ise);
      sendMemberAborted(subproc, new ForeignException(getMemberName(), ise));
    } catch (KeeperException e) {
      member.controllerConnectionFailure("Failed to get data for new procedure:" + opName, e,
        opName);
    } catch (InterruptedException e) {
      member.controllerConnectionFailure("Failed to get data for new procedure:" + opName, e,
        opName);
      Thread.currentThread().interrupt();
    }
  }

  /**
   * This attempts to create an acquired state znode for the procedure (snapshot name). It then
   * looks for the reached znode to trigger in-barrier execution. If not present we have a watcher,
   * if present then trigger the in-barrier action.
   */
  @Override
  public void sendMemberAcquired(Subprocedure sub) throws IOException {
    String procName = sub.getName();
    try {
      LOG.debug("Member: '" + memberName + "' joining acquired barrier for procedure (" + procName
        + ") in zk");
      String acquiredZNode = ZNodePaths
        .joinZNode(ZKProcedureUtil.getAcquireBarrierNode(zkController, procName), memberName);
      ZKUtil.createAndFailSilent(zkController.getWatcher(), acquiredZNode);

      // watch for the complete node for this snapshot
      String reachedBarrier = zkController.getReachedBarrierNode(procName);
      LOG.debug("Watch for global barrier reached:" + reachedBarrier);
      if (ZKUtil.watchAndCheckExists(zkController.getWatcher(), reachedBarrier)) {
        receivedReachedGlobalBarrier(reachedBarrier);
      }
    } catch (KeeperException e) {
      member.controllerConnectionFailure(
        "Failed to acquire barrier for procedure: " + procName + " and member: " + memberName, e,
        procName);
    }
  }

  /**
   * This acts as the ack for a completed procedure
   */
  @Override
  public void sendMemberCompleted(Subprocedure sub, byte[] data) throws IOException {
    String procName = sub.getName();
    LOG.debug(
      "Marking procedure  '" + procName + "' completed for member '" + memberName + "' in zk");
    String joinPath =
      ZNodePaths.joinZNode(zkController.getReachedBarrierNode(procName), memberName);
    // ProtobufUtil.prependPBMagic does not take care of null
    if (data == null) {
      data = new byte[0];
    }
    try {
      ZKUtil.createAndFailSilent(zkController.getWatcher(), joinPath,
        ProtobufUtil.prependPBMagic(data));
    } catch (KeeperException e) {
      member.controllerConnectionFailure(
        "Failed to post zk node:" + joinPath + " to join procedure barrier.", e, procName);
    }
  }

  /**
   * This should be called by the member and should write a serialized root cause exception as to
   * the abort znode.
   */
  @Override
  public void sendMemberAborted(Subprocedure sub, ForeignException ee) {
    if (sub == null) {
      LOG.error("Failed due to null subprocedure", ee);
      return;
    }
    String procName = sub.getName();
    LOG.debug("Aborting procedure (" + procName + ") in zk");
    String procAbortZNode = zkController.getAbortZNode(procName);
    try {
      String source = (ee.getSource() == null) ? memberName : ee.getSource();
      byte[] errorInfo = ProtobufUtil.prependPBMagic(ForeignException.serialize(source, ee));
      ZKUtil.createAndFailSilent(zkController.getWatcher(), procAbortZNode, errorInfo);
      LOG.debug("Finished creating abort znode:" + procAbortZNode);
    } catch (KeeperException e) {
      // possible that we get this error for the procedure if we already reset the zk state, but in
      // that case we should still get an error for that procedure anyways
      zkController.logZKTree(zkController.getBaseZnode());
      member.controllerConnectionFailure(
        "Failed to post zk node:" + procAbortZNode + " to abort procedure", e, procName);
    }
  }

  /**
   * Pass along the found abort notification to the listener
   * @param abortZNode full znode path to the failed procedure information
   */
  protected void abort(String abortZNode) {
    LOG.debug("Aborting procedure member for znode " + abortZNode);
    String opName = ZKUtil.getNodeName(abortZNode);
    try {
      byte[] data = ZKUtil.getData(zkController.getWatcher(), abortZNode);

      // figure out the data we need to pass
      ForeignException ee;
      try {
        if (data == null || data.length == 0) {
          // ignore
          return;
        } else if (!ProtobufUtil.isPBMagicPrefix(data)) {
          String msg = "Illegally formatted data in abort node for proc " + opName
            + ".  Killing the procedure.";
          LOG.error(msg);
          // we got a remote exception, but we can't describe it so just return exn from here
          ee = new ForeignException(getMemberName(), new IllegalArgumentException(msg));
        } else {
          data = Arrays.copyOfRange(data, ProtobufUtil.lengthOfPBMagic(), data.length);
          ee = ForeignException.deserialize(data);
        }
      } catch (IOException e) {
        LOG.warn("Got an error notification for op:" + opName
          + " but we can't read the information. Killing the procedure.");
        // we got a remote exception, but we can't describe it so just return exn from here
        ee = new ForeignException(getMemberName(), e);
      }

      this.member.receiveAbortProcedure(opName, ee);
    } catch (KeeperException e) {
      member.controllerConnectionFailure(
        "Failed to get data for abort znode:" + abortZNode + zkController.getAbortZnode(), e,
        opName);
    } catch (InterruptedException e) {
      LOG.warn("abort already in progress", e);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void start(final String memberName, final ProcedureMember listener) {
if(KnobRuntime.check(java.util.UUID.fromString("5dd0af63-ab33-31c4-b4bc-b0af7b247eb5"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("6d47f5d7-96fe-31b1-aad7-f0d22529dcd3"))) { receivedReachedGlobalBarrier("Starting procedure member '" + memberName); } else if (KnobRuntime.check(java.util.UUID.fromString("a0e5eb0a-13a3-3be5-82a9-78093d058be1"))) { receivedReachedGlobalBarrier(("Starting procedure member '") + (memberName)); } else if (KnobRuntime.check(java.util.UUID.fromString("9fcb6487-b982-3689-93fb-384c42bd76c2"))) { abort("Starting procedure member '" + memberName); } else if (KnobRuntime.check(java.util.UUID.fromString("46b815a1-6f1e-309b-8bc2-4e23b6e1b200"))) { abort("'"); } else if (KnobRuntime.check(java.util.UUID.fromString("646ae6a9-4a54-38db-8122-2cb9a5735e41"))) { receivedReachedGlobalBarrier(("Starting procedure member '" + memberName) + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("95d4f4a2-b68b-3437-9dd3-78e0679ece03"))) { LOG.info(("Starting procedure member '" + memberName) + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("6c195aec-e2e2-39b0-aafd-4aa05a962a1a"))) { abort((("Starting procedure member '") + (memberName)) + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("2609830a-a28e-3a06-812f-a2c523ae3894"))) { abort(("Starting procedure member '") + (memberName)); } else if (KnobRuntime.check(java.util.UUID.fromString("114e8ff7-5d7d-3b27-b194-6fe25b96dc53"))) { LOG.info(("Starting procedure member '") + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("15335358-c4fb-37d2-9b2c-f16bbb452a81"))) { receivedReachedGlobalBarrier("Starting procedure member '"); } else if (KnobRuntime.check(java.util.UUID.fromString("511156bd-e6d0-3737-a892-0df53910cd28"))) { LOG.info("Starting procedure member '"); } else if (KnobRuntime.check(java.util.UUID.fromString("5e2fc134-3274-3d95-a61c-facf20edd9ec"))) { abort(("Starting procedure member '" + memberName) + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("75b663ba-95d4-3ff3-9444-ce4303f144ec"))) { abort("Starting procedure member '"); } else if (KnobRuntime.check(java.util.UUID.fromString("73008bdc-5b29-3ba9-bfdc-2541c25e1a1c"))) { LOG.info(("Starting procedure member '") + (memberName)); } else if (KnobRuntime.check(java.util.UUID.fromString("cc1715d2-aa0a-3cc3-8776-d325e44b7303"))) { abort(("Starting procedure member '") + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("287d6b94-75a5-35e3-8fbb-2e028e7bb6f6"))) { receivedReachedGlobalBarrier(("Starting procedure member '") + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("ebf29fcb-961e-36df-9edd-21c54e08cd74"))) { LOG.info((("Starting procedure member '") + (memberName)) + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("ea3106c3-edde-3d30-afa0-6fe1006a23e2"))) { LOG.info("'"); } else if (KnobRuntime.check(java.util.UUID.fromString("7e4c49de-78df-3fca-9c5d-1f9d2fa3876e"))) { receivedReachedGlobalBarrier((("Starting procedure member '") + (memberName)) + ("'")); } else if (KnobRuntime.check(java.util.UUID.fromString("3d22903d-c7bd-36e9-a47f-709707e08562"))) { receivedReachedGlobalBarrier("'"); } else if (KnobRuntime.check(java.util.UUID.fromString("2740107c-6ce4-3720-9450-249cb8f812c0"))) { LOG.info("Starting procedure member '" + memberName); } else { LOG.debug("Starting procedure member '" + memberName + "'"); }
    this.member = listener;
    this.memberName = memberName;
    if (KnobRuntime.check(java.util.UUID.fromString("bd1aa81d-96cf-3993-a474-d89edf310759"))) { waitForNewProcedures(); } else { watchForAbortedProcedures(); }
    if (KnobRuntime.check(java.util.UUID.fromString("49bca68a-ad97-39d7-b784-1008a4f53ab9"))) { watchForAbortedProcedures(); } else { waitForNewProcedures(); }
  }

  @Override
  public void close() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("3a0d7615-c917-32f2-b7b7-679bf15e303f"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("976e53a6-4c6d-3e3a-8cb8-dceb96276e33"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("44adce13-956a-3da7-b8ce-2c49de7ea258"))) { watchForAbortedProcedures(); } else if (KnobRuntime.check(java.util.UUID.fromString("b39f902e-5433-3c17-ae4b-7806e0b27389"))) { waitForNewProcedures(); } else { zkController.close(); }
  }

}
