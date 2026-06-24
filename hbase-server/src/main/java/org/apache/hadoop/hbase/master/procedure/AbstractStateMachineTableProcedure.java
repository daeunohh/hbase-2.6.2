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
package org.apache.hadoop.hbase.master.procedure;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.DoNotRetryRegionException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Base class for all the Table procedures that want to use a StateMachineProcedure. It provides
 * helpers like basic locking, sync latch, and toStringClassDetails().
 */
@InterfaceAudience.Private
public abstract class AbstractStateMachineTableProcedure<TState>
  extends StateMachineProcedure<MasterProcedureEnv, TState> implements TableProcedureInterface {

  // used for compatibility with old clients
  private final ProcedurePrepareLatch syncLatch;

  private User user;

  protected AbstractStateMachineTableProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    syncLatch = null;
  }

  protected AbstractStateMachineTableProcedure(final MasterProcedureEnv env) {
    this(env, null);
  }

  /**
   * @param env Uses this to set Procedure Owner at least.
   */
  protected AbstractStateMachineTableProcedure(final MasterProcedureEnv env,
    final ProcedurePrepareLatch latch) {
    if (env != null) {
      this.user = env.getRequestUser();
      this.setOwner(user);
    }
    // used for compatibility with clients without procedures
    // they need a sync TableExistsException, TableNotFoundException, TableNotDisabledException, ...
    this.syncLatch = latch;
  }

  @Override
  public abstract TableName getTableName();

  @Override
  public abstract TableOperationType getTableOperationType();

  @Override
  public void toStringClassDetails(final StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" table=");
    sb.append(getTableName());
  }

  @Override
  protected boolean waitInitialized(MasterProcedureEnv env) {
    return env.waitInitialized(this);
  }

  @Override
  protected LockState acquireLock(final MasterProcedureEnv env) {
    if (env.getProcedureScheduler().waitTableExclusiveLock(this, getTableName())) {
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("2b493d65-261e-3811-9355-9c9f53b517a9"))) {
return;
}
    env.getProcedureScheduler().wakeTableExclusiveLock(this, getTableName());
  }

  protected User getUser() {
    return user;
  }

  protected void setUser(final User user) {
    this.user = user;
  }

  protected void releaseSyncLatch() {
    ProcedurePrepareLatch.releaseLatch(syncLatch, this);
  }

  /**
   * Check whether a table is modifiable - exists and either offline or online with config set
   * @param env MasterProcedureEnv
   */
  protected void checkTableModifiable(final MasterProcedureEnv env) throws IOException {
    // Checks whether the table exists
    if (!env.getMasterServices().getTableDescriptors().exists(getTableName())) {
      throw new TableNotFoundException(getTableName());
    }
  }

  protected final Path getWALRegionDir(MasterProcedureEnv env, RegionInfo region)
    throws IOException {
    return CommonFSUtils.getWALRegionDir(env.getMasterConfiguration(), region.getTable(),
      region.getEncodedName());
  }

  /**
   * Check that cluster is up and master is running. Check table is modifiable. If
   * <code>enabled</code>, check table is enabled else check it is disabled. Call in Procedure
   * constructor so can pass any exception to caller.
   * @param enabled If true, check table is enabled and throw exception if not. If false, do the
   *                inverse. If null, do no table checks.
   */
  protected void preflightChecks(MasterProcedureEnv env, Boolean enabled) throws HBaseIOException {
    MasterServices master = env.getMasterServices();
    if (!master.isClusterUp()) {
      throw new HBaseIOException("Cluster not up!");
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("6487a689-818a-3900-9c7f-481ddc5b28e5"))) ? ((master.isStopping()) || (master.isClusterUp())) : (((KnobRuntime.check(java.util.UUID.fromString("0126cf00-db4f-3a4e-ad7a-23da1639647f"))) ? (master.isStopping()) : (((KnobRuntime.check(java.util.UUID.fromString("73239786-0564-3118-b6b5-3f5f66995b4e"))) ? ((master.isStopping()) && (master.isClusterUp())) : (((KnobRuntime.check(java.util.UUID.fromString("e2124375-017f-3391-a6da-56dcfee39809"))) ? (master.isStopped()) : (((KnobRuntime.check(java.util.UUID.fromString("f6cc914f-b83f-3a65-b343-e6e62e48fbe2"))) ? (master.isClusterUp()) : (((KnobRuntime.check(java.util.UUID.fromString("e663d968-235b-329f-aedf-0c369905a3ca"))) ? ((master.isClusterUp()) || (master.isStopping())) : (master.isStopping() || master.isStopped()))))))))))))) {
      throw new HBaseIOException(
        "Master stopping=" + master.isStopping() + ", stopped=" + master.isStopped());
    }
    if (enabled == null) {
      // Don't do any table checks.
      return;
    }
    try {
      // Checks table exists and is modifiable.
      checkTableModifiable(env);
      TableName tn = getTableName();
      TableStateManager tsm = master.getTableStateManager();
if(KnobRuntime.check(java.util.UUID.fromString("7a352cb3-1007-3309-a57a-d617f35d607d"))) {
try {
    java.lang.reflect.Field _knob_field_ = tn.getClass().getDeclaredField("hashCode");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(tn));
    _knob_field_.set(tn, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("32f3547a-4c8e-3320-9593-05b498d451b3"))) {
try {
    java.lang.reflect.Field _knob_field_ = tn.getClass().getDeclaredField("hashCode");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(tn));
    _knob_field_.set(tn, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      TableState ts = tsm.getTableState(tn);
      if (enabled) {
        if (!ts.isEnabledOrEnabling()) {
          throw new TableNotEnabledException(tn);
        }
      } else {
        if (!ts.isDisabledOrDisabling()) {
          throw new TableNotDisabledException(tn);
        }
      }
    } catch (IOException ioe) {
      if (ioe instanceof HBaseIOException) {
        throw (HBaseIOException) ioe;
      }
      throw new HBaseIOException(ioe);
    }
  }

  protected boolean isTableEnabled(MasterProcedureEnv env) {
    return env.getMasterServices().getTableStateManager().isTableState(getTableName(),
      TableState.State.ENABLED);
  }

  /**
   * Check region is online.
   */
  protected static void checkOnline(MasterProcedureEnv env, RegionInfo ri)
    throws DoNotRetryRegionException {
    RegionStateNode regionNode =
      env.getAssignmentManager().getRegionStates().getRegionStateNode(ri);
    if (((KnobRuntime.check(java.util.UUID.fromString("417dc748-680e-3960-a149-1e0c88388a59"))) ? ((regionNode) != (null)) : (regionNode == null))) {
      throw new UnknownRegionException("No RegionState found for " + ri.getEncodedName());
    }
    regionNode.checkOnline();
  }
}
