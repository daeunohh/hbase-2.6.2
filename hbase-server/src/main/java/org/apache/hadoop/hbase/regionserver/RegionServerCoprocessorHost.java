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
package org.apache.hadoop.hbase.regionserver;
import org.knobinjection.runtime.KnobRuntime;

import com.google.protobuf.Service;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SharedConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.BaseEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.CoprocessorServiceBackwardCompatiblity;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.HasRegionServerServices;
import org.apache.hadoop.hbase.coprocessor.MetricsCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.coprocessor.SingletonCoprocessorService;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;

@InterfaceAudience.Private
public class RegionServerCoprocessorHost
  extends CoprocessorHost<RegionServerCoprocessor, RegionServerCoprocessorEnvironment> {

  private static final Logger LOG = LoggerFactory.getLogger(RegionServerCoprocessorHost.class);

  private RegionServerServices rsServices;

  public RegionServerCoprocessorHost(RegionServerServices rsServices, Configuration conf) {
    super(rsServices);
    this.rsServices = rsServices;
    this.conf = conf;
    // Log the state of coprocessor loading here; should appear only once or
    // twice in the daemon log, depending on HBase version, because there is
    // only one RegionServerCoprocessorHost instance in the RS process
    boolean coprocessorsEnabled =
      conf.getBoolean(COPROCESSORS_ENABLED_CONF_KEY, DEFAULT_COPROCESSORS_ENABLED);
    boolean tableCoprocessorsEnabled =
      conf.getBoolean(USER_COPROCESSORS_ENABLED_CONF_KEY, DEFAULT_USER_COPROCESSORS_ENABLED);
    if (KnobRuntime.check(java.util.UUID.fromString("8866a461-573b-3310-9e9c-7f279df83d77"))) { LOG.info((coprocessorsEnabled ? "enabled" : "disabled")); } else if (KnobRuntime.check(java.util.UUID.fromString("133ccdf6-899e-3b30-a177-502553386ce0"))) { LOG.info(("System coprocessor loading is ") + ((coprocessorsEnabled ? "enabled" : "disabled"))); } else if (KnobRuntime.check(java.util.UUID.fromString("d8cefa86-c08e-3006-897e-fcd3a52ef128"))) { LOG.info("System coprocessor loading is "); } else { LOG.info("System coprocessor loading is " + (coprocessorsEnabled ? "enabled" : "disabled")); }
    LOG.info("Table coprocessor loading is "
      + ((coprocessorsEnabled && tableCoprocessorsEnabled) ? "enabled" : "disabled"));
    loadSystemCoprocessors(conf, REGIONSERVER_COPROCESSOR_CONF_KEY);
  }

  @Override
  public RegionServerEnvironment createEnvironment(RegionServerCoprocessor instance, int priority,
    int sequence, Configuration conf) {
    // If a CoreCoprocessor, return a 'richer' environment, one laden with RegionServerServices.
    return instance.getClass().isAnnotationPresent(CoreCoprocessor.class)
      ? new RegionServerEnvironmentForCoreCoprocessors(instance, priority, sequence, conf,
        this.rsServices)
      : new RegionServerEnvironment(instance, priority, sequence, conf, this.rsServices);
  }

  @Override
  public RegionServerCoprocessor checkAndGetInstance(Class<?> implClass)
    throws InstantiationException, IllegalAccessException {
    try {
      if (RegionServerCoprocessor.class.isAssignableFrom(implClass)) {
        return implClass.asSubclass(RegionServerCoprocessor.class).getDeclaredConstructor()
          .newInstance();
      } else if (SingletonCoprocessorService.class.isAssignableFrom(implClass)) {
        // For backward compatibility with old CoprocessorService impl which don't extend
        // RegionCoprocessor.
        SingletonCoprocessorService tmp = implClass.asSubclass(SingletonCoprocessorService.class)
          .getDeclaredConstructor().newInstance();
        return new CoprocessorServiceBackwardCompatiblity.RegionServerCoprocessorService(tmp);
      } else {
        LOG.error("{} is not of type RegionServerCoprocessor. Check the configuration of {}",
          implClass.getName(), CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY);
        return null;
      }
    } catch (NoSuchMethodException | InvocationTargetException e) {
      throw (InstantiationException) new InstantiationException(implClass.getName()).initCause(e);
    }
  }

  private ObserverGetter<RegionServerCoprocessor, RegionServerObserver> rsObserverGetter =
    RegionServerCoprocessor::getRegionServerObserver;

  abstract class RegionServerObserverOperation
    extends ObserverOperationWithoutResult<RegionServerObserver> {
    public RegionServerObserverOperation() {
      super(rsObserverGetter);
    }

    public RegionServerObserverOperation(User user) {
      super(rsObserverGetter, user);
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // RegionServerObserver operations
  //////////////////////////////////////////////////////////////////////////////////////////////////

  public void preStop(String message, User user) throws IOException {
    // While stopping the region server all coprocessors method should be executed first then the
    // coprocessor should be cleaned up.
    if (coprocEnvironments.isEmpty()) {
      return;
    }
    execShutdown(new RegionServerObserverOperation(user) {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
        observer.preStopRegionServer(this);
      }

      @Override
      public void postEnvCall() {
        // invoke coprocessor stop method
        shutdown(this.getEnvironment());
      }
    });
  }

  public void preRollWALWriterRequest() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
        observer.preRollWALWriterRequest(this);
      }
    });
  }

  public void postRollWALWriterRequest() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
        observer.postRollWALWriterRequest(this);
      }
    });
  }

  public void preReplicateLogEntries() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
        observer.preReplicateLogEntries(this);
      }
    });
  }

  public void postReplicateLogEntries() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("d5d08ad6-898f-3a2f-8ef9-6f3c80e4294d"))) {
throw new java.io.IOException("Injected exception");
}
        observer.postReplicateLogEntries(this);
      }
    });
  }

  public void preReplicationSinkBatchMutate(AdminProtos.WALEntry walEntry, Mutation mutation)
    throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("99f268a3-2d71-35dd-a1a2-97084906d24d"))) {
try {
    java.lang.reflect.Field field = walEntry.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walEntry));
    field.set(walEntry, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("32aa0ba3-b3a9-3ff8-9d79-c1b011e16db1"))) {
try {
    java.lang.reflect.Field field = walEntry.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walEntry));
    field.set(walEntry, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1b5a2869-80d7-30bd-a603-6649c1dfa9da"))) {
try {
    java.lang.reflect.Field field = walEntry.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walEntry));
    field.set(walEntry, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3b07d2d0-e155-3507-bc9c-dff65f6df80e"))) {
try {
    java.lang.reflect.Field field = walEntry.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walEntry));
    field.set(walEntry, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a3a4fc1d-411e-3c0d-a7d9-f99f2acb0f2b"))) {
try {
    java.lang.reflect.Field field = walEntry.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walEntry));
    field.set(walEntry, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("17ec4daa-c327-3af9-adcf-36d06df653db"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("ts");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b2d0bf8c-eb51-3891-8c70-3018748cc1b6"))) {
try {
    java.lang.reflect.Field field = walEntry.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walEntry));
    field.set(walEntry, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cd681d0d-ea0d-3bcc-9404-8094f386c80a"))) {
try {
    java.lang.reflect.Field field = walEntry.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walEntry));
    field.set(walEntry, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2fceb967-56bb-3510-b295-941770648015"))) {
try {
    java.lang.reflect.Field field = walEntry.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walEntry));
    field.set(walEntry, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9204bbcc-d321-3a5a-87c1-cfc49fa477a6"))) {
throw new java.io.IOException("Injected exception");
}
        observer.preReplicationSinkBatchMutate(this, walEntry, mutation);
      }
    });
  }

  public void postReplicationSinkBatchMutate(AdminProtos.WALEntry walEntry, Mutation mutation)
    throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("9301e74c-4cca-3a45-b75e-d79f33fb4dea"))) {
try {
    java.lang.reflect.Field field = walEntry.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walEntry));
    field.set(walEntry, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5d0438bc-558e-3d17-9f5e-52bf3508ddcc"))) {
try {
    java.lang.reflect.Field field = walEntry.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walEntry));
    field.set(walEntry, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0e9d622b-7514-3eb5-85a3-26bc9c4f5393"))) {
try {
    java.lang.reflect.Field field = mutation.getClass().getDeclaredField("ts");
    field.setAccessible(true);
    long oldValue = ((long)field.get(mutation));
    field.set(mutation, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0812c4b9-61e3-3653-979d-45b6a39ae898"))) {
try {
    java.lang.reflect.Field field = walEntry.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walEntry));
    field.set(walEntry, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("921e02cc-bf5b-35a4-aaa4-08d554430592"))) {
try {
    java.lang.reflect.Field field = walEntry.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walEntry));
    field.set(walEntry, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8e51c07f-0258-37d9-baa5-ff99dd1665e7"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("0aba4998-4245-3064-beec-02d3ce479c24"))) {
try {
    java.lang.reflect.Field field = walEntry.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walEntry));
    field.set(walEntry, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a7ec3c90-f9f2-37ba-b10a-f695fb5a1aa1"))) {
try {
    java.lang.reflect.Field field = walEntry.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walEntry));
    field.set(walEntry, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3ee43324-7de0-37fa-bd1f-b56a32d53bd3"))) {
try {
    java.lang.reflect.Field field = walEntry.getClass().getDeclaredField("bitField0_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walEntry));
    field.set(walEntry, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("757bafd3-820a-3441-b550-ff7ef09d7b53"))) {
try {
    java.lang.reflect.Field field = walEntry.getClass().getDeclaredField("associatedCellCount_");
    field.setAccessible(true);
    int oldValue = ((int)field.get(walEntry));
    field.set(walEntry, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        observer.postReplicationSinkBatchMutate(this, walEntry, mutation);
      }
    });
  }

  public ReplicationEndpoint postCreateReplicationEndPoint(final ReplicationEndpoint endpoint)
    throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return endpoint;
    }
    return execOperationWithResult(
      new ObserverOperationWithResult<RegionServerObserver, ReplicationEndpoint>(rsObserverGetter,
        endpoint) {
        @Override
        public ReplicationEndpoint call(RegionServerObserver observer) throws IOException {
          return observer.postCreateReplicationEndPoint(this, getResult());
        }
      });
  }

  public void preClearCompactionQueues() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
        observer.preClearCompactionQueues(this);
      }
    });
  }

  public void postClearCompactionQueues() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
        observer.postClearCompactionQueues(this);
      }
    });
  }

  public void preExecuteProcedures() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
        observer.preExecuteProcedures(this);
      }
    });
  }

  public void postExecuteProcedures() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
        observer.postExecuteProcedures(this);
      }
    });
  }

  public void preUpdateConfiguration(Configuration preReloadConf) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
        observer.preUpdateRegionServerConfiguration(this, preReloadConf);
      }
    });
  }

  public void postUpdateConfiguration(Configuration postReloadConf) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
        observer.postUpdateRegionServerConfiguration(this, postReloadConf);
      }
    });
  }

  public void preClearRegionBlockCache() throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
        observer.preClearRegionBlockCache(this);
      }
    });
  }

  public void postClearRegionBlockCache(CacheEvictionStats stats) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionServerObserverOperation() {
      @Override
      public void call(RegionServerObserver observer) throws IOException {
        observer.postClearRegionBlockCache(this, stats);
      }
    });
  }

  /**
   * Coprocessor environment extension providing access to region server related services.
   */
  private static class RegionServerEnvironment extends BaseEnvironment<RegionServerCoprocessor>
    implements RegionServerCoprocessorEnvironment {
    private final MetricRegistry metricRegistry;
    private final RegionServerServices services;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "BC_UNCONFIRMED_CAST",
        justification = "Intentional; FB has trouble detecting isAssignableFrom")
    public RegionServerEnvironment(final RegionServerCoprocessor impl, final int priority,
      final int seq, final Configuration conf, final RegionServerServices services) {
      super(impl, priority, seq, conf);
      // If coprocessor exposes any services, register them.
      for (Service service : impl.getServices()) {
        services.registerService(service);
      }
      this.services = services;
      this.metricRegistry =
        MetricsCoprocessor.createRegistryForRSCoprocessor(impl.getClass().getName());
    }

    @Override
    public OnlineRegions getOnlineRegions() {
      return this.services;
    }

    @Override
    public ServerName getServerName() {
      return this.services.getServerName();
    }

    @Override
    public Connection getConnection() {
      return new SharedConnection(this.services.getConnection());
    }

    @Override
    public Connection createConnection(Configuration conf) throws IOException {
      return this.services.createConnection(conf);
    }

    @Override
    public MetricRegistry getMetricRegistryForRegionServer() {
      return metricRegistry;
    }

    @Override
    public void shutdown() {
      super.shutdown();
      MetricsCoprocessor.removeRegistry(metricRegistry);
    }
  }

  /**
   * Special version of RegionServerEnvironment that exposes RegionServerServices for Core
   * Coprocessors only. Temporary hack until Core Coprocessors are integrated into Core.
   */
  private static class RegionServerEnvironmentForCoreCoprocessors extends RegionServerEnvironment
    implements HasRegionServerServices {
    final RegionServerServices regionServerServices;

    public RegionServerEnvironmentForCoreCoprocessors(final RegionServerCoprocessor impl,
      final int priority, final int seq, final Configuration conf,
      final RegionServerServices services) {
      super(impl, priority, seq, conf, services);
      this.regionServerServices = services;
    }

    /**
     * @return An instance of RegionServerServices, an object NOT for general user-space Coprocessor
     *         consumption.
     */
    @Override
    public RegionServerServices getRegionServerServices() {
      return this.regionServerServices;
    }
  }
}
