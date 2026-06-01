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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.replication.ReplicationPeerManager;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.store.LeaseRecovery;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.RecoverLeaseFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MasterProcedureEnv implements ConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(MasterProcedureEnv.class);

  @InterfaceAudience.Private
  public static class FsUtilsLeaseRecovery implements LeaseRecovery {
    private final MasterServices master;

    public FsUtilsLeaseRecovery(final MasterServices master) {
      this.master = master;
    }

    @Override
    public void recoverFileLease(final FileSystem fs, final Path path) throws IOException {
      final Configuration conf = master.getConfiguration();
      RecoverLeaseFSUtils.recoverFileLease(fs, path, conf, new CancelableProgressable() {
        @Override
        public boolean progress() {
          LOG.debug("Recover Procedure Store log lease: " + path);
          return isRunning();
        }
      });
    }

    private boolean isRunning() {
      return !master.isStopped() && !master.isStopping() && !master.isAborted();
    }
  }

  private final RSProcedureDispatcher remoteDispatcher;
  private final MasterProcedureScheduler procSched;
  private final MasterServices master;

  public MasterProcedureEnv(final MasterServices master,
    final RSProcedureDispatcher remoteDispatcher) {
    this.master = master;
    this.procSched = new MasterProcedureScheduler(
      procId -> master.getMasterProcedureExecutor().getProcedure(procId));
    this.remoteDispatcher = remoteDispatcher;
  }

  public User getRequestUser() {
    return RpcServer.getRequestUser().orElse(Superusers.getSystemUser());
  }

  public MasterServices getMasterServices() {
if(KnobRuntime.check(java.util.UUID.fromString("a298fd19-3c33-329e-a89e-5dd8526640c6"))) {
return null;
}
    return master;
  }

  public Configuration getMasterConfiguration() {
if(KnobRuntime.check(java.util.UUID.fromString("eb75cbd4-437f-3aa9-835c-3f3a3adbddfe"))) {
return null;
}
    return master.getConfiguration();
  }

  public AssignmentManager getAssignmentManager() {
if(KnobRuntime.check(java.util.UUID.fromString("599e354f-4d69-3180-9cf7-a9f4fcd2b0d0"))) {
return null;
}
    return master.getAssignmentManager();
  }

  public MasterCoprocessorHost getMasterCoprocessorHost() {
if(KnobRuntime.check(java.util.UUID.fromString("559df3a8-5559-3922-9137-669bdb25ec6c"))) {
return null;
}
    return master.getMasterCoprocessorHost();
  }

  public MasterProcedureScheduler getProcedureScheduler() {
if(KnobRuntime.check(java.util.UUID.fromString("de2d5234-9f3d-38f6-90d9-33f243ec84ad"))) {
return null;
}
    return procSched;
  }

  public RSProcedureDispatcher getRemoteDispatcher() {
if(KnobRuntime.check(java.util.UUID.fromString("5e5a22bf-bd5f-3f12-93bc-ece08ecdf600"))) {
return null;
}
    return remoteDispatcher;
  }

  public ReplicationPeerManager getReplicationPeerManager() {
if(KnobRuntime.check(java.util.UUID.fromString("931d74a8-0771-3f9c-8861-6da888bead7c"))) {
return null;
}
    return master.getReplicationPeerManager();
  }

  public MasterFileSystem getMasterFileSystem() {
if(KnobRuntime.check(java.util.UUID.fromString("5c245d67-6266-3671-9f47-d28d3c40d7ca"))) {
return null;
}
    return master.getMasterFileSystem();
  }

  public boolean isRunning() {
if(KnobRuntime.check(java.util.UUID.fromString("d6545599-b751-3a75-887b-46e6b6375e81"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("61626cc9-7c65-336b-8743-24d95be392af"))) {
return false;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("471d065f-aec9-3962-996a-a78dfcc3765e"))) ? (((this.master) != (null)) && ((this.master.getMasterProcedureExecutor()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("271b5d56-bc21-3383-b183-38c7d2afa8aa"))) ? (((this.master) != (null)) || (this.master.getMasterProcedureExecutor() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("5bc1c91a-398b-317f-a823-f123d594973b"))) ? (((this.master) == (null)) || (this.master.getMasterProcedureExecutor() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("825b7c00-3e4c-39d8-9629-5905d6cbe579"))) ? ((this.master == null) || ((this.master.getMasterProcedureExecutor()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("6080571b-6072-394e-83ef-37ed2f6f3ba6"))) ? (this.master == null) : (((KnobRuntime.check(java.util.UUID.fromString("dde7dc48-bba8-3cc1-8da6-0df1115c7d44"))) ? (((this.master) == (null)) && ((this.master.getMasterProcedureExecutor()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("89c86d15-fdd0-3157-b13c-75865c035071"))) ? (((this.master) != (null)) || ((this.master.getMasterProcedureExecutor()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("685640db-bf56-36b9-a9e7-a7a0e3510bd3"))) ? ((this.master.getMasterProcedureExecutor()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("a33925ad-6356-32d6-8fb9-fb490353bcaa"))) ? (((this.master) == (null)) && ((this.master.getMasterProcedureExecutor()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("5c2fbd4e-0282-385c-b694-f05a7b43c72e"))) ? (((this.master) != (null)) || ((this.master.getMasterProcedureExecutor()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("5ac708db-a4d7-36c7-9f33-fef9e403e2d2"))) ? ((this.master) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("705539b7-9c97-3995-9948-fd6f32ec8b68"))) ? (this.master.getMasterProcedureExecutor() == null) : (((KnobRuntime.check(java.util.UUID.fromString("42ab0720-58dc-31fe-841c-f28827eed7c3"))) ? ((this.master.getMasterProcedureExecutor()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("48759c03-2564-38ec-82da-786c749541e4"))) ? (((this.master) != (null)) && (this.master.getMasterProcedureExecutor() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("4d1ea4c5-e5df-3228-bf72-f3872fc46afa"))) ? ((this.master == null) && (this.master.getMasterProcedureExecutor() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("8626be87-dcd5-3759-9b77-aae5730f7fad"))) ? (((this.master) != (null)) && ((this.master.getMasterProcedureExecutor()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("4f67c8e1-3010-34e2-9616-bc03ba605e92"))) ? ((this.master == null) && ((this.master.getMasterProcedureExecutor()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("1748c9ef-2e17-3739-9719-f20b1a994bb5"))) ? (((this.master) == (null)) || ((this.master.getMasterProcedureExecutor()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("22e9ae00-c3b8-36f7-b942-754a188e0fe0"))) ? ((this.master) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("df56c082-3d16-3815-abd7-614289ab042d"))) ? ((this.master == null) || (this.master.getMasterProcedureExecutor() == null)) : (((KnobRuntime.check(java.util.UUID.fromString("6f44a66d-1233-3550-9e9d-d81e7f6868d6"))) ? (((this.master) == (null)) || ((this.master.getMasterProcedureExecutor()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("177829c7-66b8-3e9f-9ddf-3d88bb9ebfd9"))) ? ((this.master == null) && ((this.master.getMasterProcedureExecutor()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("58b17adc-77ed-36d7-807e-1d2dc23086d4"))) ? ((this.master == null) || ((this.master.getMasterProcedureExecutor()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("02d63719-98ab-3528-8146-9c1751f7d202"))) ? (((this.master) == (null)) && (this.master.getMasterProcedureExecutor() == null)) : (this.master == null || this.master.getMasterProcedureExecutor() == null))))))))))))))))))))))))))))))))))))))))))))))))) return false;
    return master.getMasterProcedureExecutor().isRunning();
  }

  public boolean isInitialized() {
    return master.isInitialized();
  }

  public boolean waitInitialized(Procedure<?> proc) {
if(KnobRuntime.check(java.util.UUID.fromString("73fda70b-785f-35d9-9243-efcb8cefb89e"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("submittedTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("61e89539-f158-3fda-a013-317ab669ced9"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("40434f2e-2953-3723-83fc-8ae71854e3ac"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ff9c2d37-ab7d-3421-a34f-d4a75fe077ed"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("725df5c0-ba2f-3ea5-9968-bed6b4a049dd"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bd968a4b-95e5-36f6-8251-edc0fbb5223a"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("wasExecuted");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(proc);
    field.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5d1a5a8d-d62d-3621-8c23-77d40ccfc804"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("67f02ea1-c096-351c-ac31-b136b3247433"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("parentProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("adf936d1-310f-38a9-9243-5f34f155a301"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("bypass");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(proc);
    field.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("255c1828-6620-3def-867c-ba66109afb6a"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("ac6ae464-3021-3e0d-b5b8-6646cc2bd133"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a4a2a781-96e7-3c0f-b6dd-24df9c59d598"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("locked");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(proc);
    field.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fc247984-83e4-3936-8627-fc7fa5200cd8"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("be2589b0-e7bc-3407-b9bc-4d120fc5f356"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7cdf4eff-7f21-31e5-b801-c93da0780cd1"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5b71f5f8-81a1-3e41-b80a-db164c5def45"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("rootProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5bf72663-e092-351e-9cca-3ca019702384"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0afbecdd-eabd-34e8-966d-069e81ad1931"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("fbc50a3e-5f76-3022-8e0b-32a4c70f4cd3"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6dc5a7c4-0780-3e9c-9292-c04540526c72"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(proc));
    field.set(proc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("de013ff3-3977-3373-9217-b1a8f21706c4"))) {
try {
    java.lang.reflect.Field field = proc.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(proc));
    field.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return master.getInitializedEvent().suspendIfNotReady(proc);
  }

  public void setEventReady(ProcedureEvent<?> event, boolean isReady) {
    if (isReady) {
      event.wake(procSched);
    } else {
      event.suspend();
    }
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("8f610cc3-1cf9-3fd0-9ba4-a98ce53481ff"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("bc63f2b2-77b7-37ee-ac6d-abfe6cdd2334"))) {
try {
    java.lang.reflect.Field field = conf.getClass().getDeclaredField("loadDefaults");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(conf);
    field.set(conf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    master.getMasterProcedureExecutor().refreshConfiguration(conf);
  }
}
