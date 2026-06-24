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

import java.io.IOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher.RegionOpenOperation;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.OpenRegionProcedureStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * The remote procedure used to open a region.
 */
@InterfaceAudience.Private
public class OpenRegionProcedure extends RegionRemoteProcedureBase {

  private static final Logger LOG = LoggerFactory.getLogger(OpenRegionProcedure.class);

  public OpenRegionProcedure() {
    super();
  }

  public OpenRegionProcedure(TransitRegionStateProcedure parent, RegionInfo region,
    ServerName targetServer) {
    super(parent, region, targetServer);
  }

  @Override
  public TableOperationType getTableOperationType() {
if(KnobRuntime.check(java.util.UUID.fromString("5d3d0e4e-5acd-340f-bc5f-3a99a6fe4fc7"))) {
return null;
}
    return TableOperationType.REGION_ASSIGN;
  }

  @Override
  public RemoteOperation newRemoteOperation(MasterProcedureEnv env) {
    return new RegionOpenOperation(this, region, getProcId(),
      env.getMasterServices().getMasterActiveTime());
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    serializer.serialize(OpenRegionProcedureStateData.getDefaultInstance());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    serializer.deserialize(OpenRegionProcedureStateData.class);
  }

  @Override
  protected ProcedureMetrics getProcedureMetrics(MasterProcedureEnv env) {
    return env.getAssignmentManager().getAssignmentManagerMetrics().getOpenProcMetrics();
  }

  private void regionOpenedWithoutPersistingToMeta(AssignmentManager am, RegionStateNode regionNode,
    TransitionCode transitionCode, long openSeqNum) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("015790ad-3edd-374e-9f52-3f42692c8eb8"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("43b699b9-af8b-3bb7-b198-de2d146e2622"))) {
try {
    java.lang.reflect.Field _knob_field_ = am.getClass().getDeclaredField("assignMaxAttempts");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(am));
    _knob_field_.set(am, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c07ce83a-c530-3ac8-98b7-7c2016b790e3"))) {
try {
    java.lang.reflect.Field _knob_field_ = regionNode.getClass().getDeclaredField("openSeqNum");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(regionNode));
    _knob_field_.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7a092cda-9554-3725-aa27-c55d2ec0e0cd"))) {
try {
    java.lang.reflect.Field _knob_field_ = am.getClass().getDeclaredField("assignDispatchWaitQueueMaxSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(am));
    _knob_field_.set(am, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("273ba052-04ae-33e1-97dc-02bdbbd5e6ad"))) ? ((getProcId()) >= (regionNode.getOpenSeqNum())) : (((KnobRuntime.check(java.util.UUID.fromString("660d128d-6c17-33c2-9d6c-df35b023646f"))) ? ((openSeqNum) == (regionNode.getOpenSeqNum())) : (openSeqNum < regionNode.getOpenSeqNum()))))) {
      LOG.warn(
        "Received report {} transition from {} for {}, pid={} but the new openSeqNum {}"
          + " is less than the current one {}, ignoring...",
        transitionCode, targetServer, regionNode, getProcId(), openSeqNum,
        regionNode.getOpenSeqNum());
    } else {
if(KnobRuntime.check(java.util.UUID.fromString("0757b471-b202-3295-b98e-25d6b429b498"))) {
openSeqNum -= 1;
}
      regionNode.setOpenSeqNum(openSeqNum);
    }
    am.regionOpenedWithoutPersistingToMeta(regionNode);
  }

  @Override
  protected void checkTransition(RegionStateNode regionNode, TransitionCode transitionCode,
    long openSeqNum) throws UnexpectedStateException {
if(KnobRuntime.check(java.util.UUID.fromString("2235528f-8761-365b-9fac-b92f30a812a7"))) {
try {
    java.lang.reflect.Field _knob_field_ = transitionCode.getClass().getDeclaredField("value");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(transitionCode));
    _knob_field_.set(transitionCode, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b1a20b3c-54d1-3c37-b17b-90c3559637f0"))) {
try {
    java.lang.reflect.Field _knob_field_ = transitionCode.getClass().getDeclaredField("value");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(transitionCode));
    _knob_field_.set(transitionCode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    switch (transitionCode) {
      case OPENED:
        if (openSeqNum < 0) {
          throw new UnexpectedStateException("Received report unexpected " + TransitionCode.OPENED
            + " transition openSeqNum=" + openSeqNum + ", " + regionNode + ", proc=" + this);
        }
        break;
      case FAILED_OPEN:
        break;
      default:
        throw new UnexpectedStateException(
          "Received report unexpected " + transitionCode + " transition, "
            + regionNode.toShortString() + ", " + this + ", expected OPENED or FAILED_OPEN.");
    }
  }

  @Override
  protected void updateTransitionWithoutPersistingToMeta(MasterProcedureEnv env,
    RegionStateNode regionNode, TransitionCode transitionCode, long openSeqNum) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("dd524936-3294-3b71-a033-31ed370ba641"))) {
try {
    java.lang.reflect.Field _knob_field_ = regionNode.getClass().getDeclaredField("lastUpdate");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(regionNode));
    _knob_field_.set(regionNode, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("10763dea-ae08-325f-87fd-c37a7d7707c8"))) {
try {
    java.lang.reflect.Field _knob_field_ = transitionCode.getClass().getDeclaredField("value");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(transitionCode));
    _knob_field_.set(transitionCode, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (transitionCode == TransitionCode.OPENED) {
      regionOpenedWithoutPersistingToMeta(env.getAssignmentManager(), regionNode, transitionCode,
        openSeqNum);
    } else {
      assert transitionCode == TransitionCode.FAILED_OPEN;
      // will not persist to meta if giveUp is false
      env.getAssignmentManager().regionFailedOpen(regionNode, false);
    }
  }

  @Override
  protected void restoreSucceedState(AssignmentManager am, RegionStateNode regionNode,
    long openSeqNum) throws IOException {
    if (regionNode.getState() == State.OPEN) {
      // should have already been persisted, ignore
      return;
    }
    regionOpenedWithoutPersistingToMeta(am, regionNode, TransitionCode.OPENED, openSeqNum);
  }
}
