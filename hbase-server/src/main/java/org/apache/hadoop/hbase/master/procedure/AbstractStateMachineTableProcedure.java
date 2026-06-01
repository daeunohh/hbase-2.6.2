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
    if (((KnobRuntime.check(java.util.UUID.fromString("9633fdc9-5790-3fff-bf19-5fc0f207f6ec"))) ? ((env) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f52e7d52-c934-33dc-ad0c-030d5f776649"))) ? ((env) != (null)) : (env != null))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("843e97e4-42e2-349e-81b1-59e0c479ac97"))) {
return;
}
    sb.append(getClass().getSimpleName());
    sb.append(" table=");
    sb.append(getTableName());
  }

  @Override
  protected boolean waitInitialized(MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("be767982-c2bb-34b8-a771-d11e8209a8f2"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("8e128ec0-9de8-3351-9b95-9075127510d6"))) {
return false;
}
    return env.waitInitialized(this);
  }

  @Override
  protected LockState acquireLock(final MasterProcedureEnv env) {
if(KnobRuntime.check(java.util.UUID.fromString("2334cc04-fe31-3658-8ad9-b0e7258b93f0"))) {
return null;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("4860516c-fee0-3f74-a2ae-aa26e431ec59"))) {
return null;
}
    return user;
  }

  protected void setUser(final User user) {
    this.user = user;
  }

  protected void releaseSyncLatch() {
if(KnobRuntime.check(java.util.UUID.fromString("2cf4118e-91cb-35d1-93e7-10c956ef8c3c"))) {
return;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("f8c088e9-b99d-3bb5-83f1-38c6a74d20b5"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("52910700-0bd4-373b-9df9-65119d2872f2"))) {
throw new java.io.IOException("Injected exception");
}
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
if(KnobRuntime.check(java.util.UUID.fromString("4c7c46ca-46df-34e6-8e76-cc58396fb36d"))) {
return;
}
    MasterServices master = env.getMasterServices();
    if (!master.isClusterUp()) {
      throw new HBaseIOException("Cluster not up!");
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("14f71ea2-6d79-3d51-ae9f-0f7417d8a037"))) ? ((master.isStopping()) && (master.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("69f162ec-1e4d-3e2e-ae3d-e077d2765ede"))) ? ((master.isStopping()) && (master.isStopping())) : (((KnobRuntime.check(java.util.UUID.fromString("af669572-ecfb-3dc9-936c-951f4be59011"))) ? ((master.isStopped()) || (master.isStopping())) : (((KnobRuntime.check(java.util.UUID.fromString("6c69405b-82bf-3371-81fd-617308a762f5"))) ? ((master.isStopped()) || (master.isClusterUp())) : (((KnobRuntime.check(java.util.UUID.fromString("093e08d3-d597-3c14-8257-6ddfee3eed22"))) ? ((master.isClusterUp()) && (master.isClusterUp())) : (((KnobRuntime.check(java.util.UUID.fromString("e0aa51ed-a6f0-3d6a-84f4-3deb1c52f8bf"))) ? ((master.isStopping()) || (master.isStopping())) : (((KnobRuntime.check(java.util.UUID.fromString("1198ee6e-5cfc-3e65-8493-1cabd306f0a2"))) ? ((master.isStopped()) && (master.isClusterUp())) : (((KnobRuntime.check(java.util.UUID.fromString("d8943263-76fc-3d8c-aee5-a2af2d13935f"))) ? ((master.isStopping()) || (master.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("f6cc914f-b83f-3a65-b343-e6e62e48fbe2"))) ? (master.isClusterUp()) : (((KnobRuntime.check(java.util.UUID.fromString("e663d968-235b-329f-aedf-0c369905a3ca"))) ? ((master.isClusterUp()) || (master.isStopping())) : (((KnobRuntime.check(java.util.UUID.fromString("6487a689-818a-3900-9c7f-481ddc5b28e5"))) ? ((master.isStopping()) || (master.isClusterUp())) : (((KnobRuntime.check(java.util.UUID.fromString("d6802ffb-af45-31f2-8566-48161c538258"))) ? ((master.isClusterUp()) && (master.isStopping())) : (((KnobRuntime.check(java.util.UUID.fromString("8f9f0ade-80c2-3d77-88e0-cd72c654b91c"))) ? ((master.isClusterUp()) || (master.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("50f8b9b3-c7b9-3ba2-82bd-8bfc99798504"))) ? ((master.isClusterUp()) || (master.isClusterUp())) : (((KnobRuntime.check(java.util.UUID.fromString("f49751c7-8503-3cc7-93df-42b6626d3624"))) ? ((master.isStopped()) || (master.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("73239786-0564-3118-b6b5-3f5f66995b4e"))) ? ((master.isStopping()) && (master.isClusterUp())) : (((KnobRuntime.check(java.util.UUID.fromString("0126cf00-db4f-3a4e-ad7a-23da1639647f"))) ? (master.isStopping()) : (((KnobRuntime.check(java.util.UUID.fromString("e2124375-017f-3391-a6da-56dcfee39809"))) ? (master.isStopped()) : (((KnobRuntime.check(java.util.UUID.fromString("7a97bbe1-2448-33f9-88ee-027580daae60"))) ? ((master.isClusterUp()) && (master.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("82af0e52-f047-3639-929d-38f07c23e58a"))) ? ((master.isStopped()) && (master.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("68bdd3ae-f7ad-3a94-9121-11ab8d92b953"))) ? ((master.isStopped()) && (master.isStopping())) : (master.isStopping() || master.isStopped()))))))))))))))))))))))))))))))))))))))))))) {
      throw new HBaseIOException(
        "Master stopping=" + master.isStopping() + ", stopped=" + master.isStopped());
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("2d2a30aa-b8e8-3420-877c-0b850df954d5"))) ? ((enabled) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("ab0897b6-2f30-35e3-b771-b71f0379701e"))) ? ((enabled) != (null)) : (enabled == null))))) {
      // Don't do any table checks.
      return;
    }
    try {
      // Checks table exists and is modifiable.
if(KnobRuntime.check(java.util.UUID.fromString("b99cb2ed-64cc-3bba-94c3-7f060fede060"))) {
throw new java.io.IOException("Injected exception");
}
      checkTableModifiable(env);
      TableName tn = getTableName();
      TableStateManager tsm = master.getTableStateManager();
if(KnobRuntime.check(java.util.UUID.fromString("c9f483be-a803-334a-a790-6e9c75dce0b7"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cb8740c9-76fd-303e-954a-5d288790f809"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("5e3face2-58be-350b-8b2a-bf4a96750dd0"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cb860606-fd14-30e4-8acd-f0d9fd2999a4"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b183dbd7-8dbf-3b38-b41e-6e7ad1753b2d"))) {
try {
    java.lang.reflect.Field field = tn.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tn));
    field.set(tn, oldValue + 1);
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
if(KnobRuntime.check(java.util.UUID.fromString("6edea470-4f22-3c80-87cd-24f507aa3199"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("a07da658-ac6e-3667-9066-fc3860455626"))) {
return false;
}
    return env.getMasterServices().getTableStateManager().isTableState(getTableName(),
      TableState.State.ENABLED);
  }

  /**
   * Check region is online.
   */
  protected static void checkOnline(MasterProcedureEnv env, RegionInfo ri)
    throws DoNotRetryRegionException {
if(KnobRuntime.check(java.util.UUID.fromString("1b47f230-09c3-36e7-9701-9e99a52518bb"))) {
return;
}
    RegionStateNode regionNode =
      env.getAssignmentManager().getRegionStates().getRegionStateNode(ri);
    if (((KnobRuntime.check(java.util.UUID.fromString("fc48cee0-a3c9-3132-bef9-a6f705b2e94d"))) ? ((regionNode) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("417dc748-680e-3960-a149-1e0c88388a59"))) ? ((regionNode) != (null)) : (regionNode == null))))) {
      throw new UnknownRegionException("No RegionState found for " + ri.getEncodedName());
    }
    regionNode.checkOnline();
  }
}
