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
package org.apache.hadoop.hbase.executor;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.io.Writer;
import java.lang.management.ThreadInfo;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.monitoring.ThreadMonitoring;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ListeningScheduledExecutorService;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This is a generic executor service. This component abstracts a threadpool, a queue to which
 * {@link EventType}s can be submitted, and a <code>Runnable</code> that handles the object that is
 * added to the queue.
 * <p>
 * In order to create a new service, create an instance of this class and then do:
 * <code>instance.startExecutorService(executorConfig);</code>. {@link ExecutorConfig} wraps the
 * configuration needed by this service. When done call {@link #shutdown()}.
 * <p>
 * In order to use the service created above, call {@link #submit(EventHandler)}.
 */
@InterfaceAudience.Private
public class ExecutorService {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorService.class);

  // hold the all the executors created in a map addressable by their names
  private final ConcurrentMap<String, Executor> executorMap = new ConcurrentHashMap<>();

  // Name of the server hosting this executor service.
  private final String servername;

  private final ListeningScheduledExecutorService delayedSubmitTimer =
    MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
      .setDaemon(true).setNameFormat("Event-Executor-Delay-Submit-Timer").build()));

  /**
   * Default constructor.
   * @param servername Name of the hosting server.
   */
  public ExecutorService(final String servername) {
    this.servername = servername;
  }

  /**
   * Start an executor service with a given name. If there was a service already started with the
   * same name, this throws a RuntimeException.
   * @param config Configuration to use for the executor.
   */
  public void startExecutorService(final ExecutorConfig config) {
if(KnobRuntime.check(java.util.UUID.fromString("3206cc02-2753-3658-9c79-3c1e4caee8ba"))) {
try {
    java.lang.reflect.Field field = config.getClass().getDeclaredField("keepAliveTimeMillis");
    field.setAccessible(true);
    long oldValue = ((long)field.get(config));
    field.set(config, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("457ac041-38ab-3067-b662-53464422df36"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("eb652e9d-7a24-38e4-894a-b32afa99aad2"))) {
try {
    java.lang.reflect.Field field = config.getClass().getDeclaredField("corePoolSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(config));
    field.set(config, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("597d0180-db39-3652-9cbe-d43f1c1e99fb"))) {
try {
    java.lang.reflect.Field field = config.getClass().getDeclaredField("corePoolSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(config));
    field.set(config, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e5c81bc3-937a-3b20-abf8-a8d2c75cf005"))) {
try {
    java.lang.reflect.Field field = config.getClass().getDeclaredField("corePoolSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(config));
    field.set(config, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a9905f49-d4f5-3e03-bea9-2290f8ed9d0c"))) {
try {
    java.lang.reflect.Field field = config.getClass().getDeclaredField("allowCoreThreadTimeout");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(config);
    field.set(config, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("55a8af44-a067-34f9-835f-9d938d81b39a"))) {
try {
    java.lang.reflect.Field field = config.getClass().getDeclaredField("corePoolSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(config));
    field.set(config, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    final String name = config.getName();
    Executor hbes = this.executorMap.compute(name, (key, value) -> {
      if (value != null) {
        throw new RuntimeException(
          "An executor service with the name " + key + " is already running!");
      }
      return new Executor(config);
    });

    LOG.debug("Starting executor service name={}, corePoolSize={}, maxPoolSize={}", name,
      hbes.threadPoolExecutor.getCorePoolSize(), hbes.threadPoolExecutor.getMaximumPoolSize());
  }

  boolean isExecutorServiceRunning(String name) {
    return this.executorMap.containsKey(name);
  }

  public void shutdown() {
if(KnobRuntime.check(java.util.UUID.fromString("c4e860fe-6201-3ec4-897b-7cff4da5d19c"))) {
return;
}
    this.delayedSubmitTimer.shutdownNow();
    for (Entry<String, Executor> entry : this.executorMap.entrySet()) {
      List<Runnable> wasRunning = entry.getValue().threadPoolExecutor.shutdownNow();
      if (!wasRunning.isEmpty()) {
        LOG.info(entry.getValue() + " had " + wasRunning + " on shutdown");
      }
    }
    this.executorMap.clear();
  }

  Executor getExecutor(final ExecutorType type) {
if(KnobRuntime.check(java.util.UUID.fromString("5b9fe12a-6d63-3fdf-ac21-3b03d42c0765"))) {
return null;
}
    return getExecutor(type.getExecutorName(this.servername));
  }

  Executor getExecutor(String name) {
if(KnobRuntime.check(java.util.UUID.fromString("21ac8ae0-e0a4-3348-84fe-43862ae274de"))) {
return null;
}
    return this.executorMap.get(name);
  }

  public ThreadPoolExecutor getExecutorThreadPool(final ExecutorType type) {
    return getExecutor(type).getThreadPoolExecutor();
  }

  /**
   * Initialize the executor lazily, Note if an executor need to be initialized lazily, then all
   * paths should use this method to get the executor, should not start executor by using
   * {@link ExecutorService#startExecutorService(ExecutorConfig)}
   */
  public ThreadPoolExecutor getExecutorLazily(ExecutorConfig config) {
    return executorMap.computeIfAbsent(config.getName(), (executorName) -> new Executor(config))
      .getThreadPoolExecutor();
  }

  public void submit(final EventHandler eh) {
if(KnobRuntime.check(java.util.UUID.fromString("360ca4e1-dc48-3ab4-9b25-5296b42a3e22"))) {
try {
    java.lang.reflect.Field field = eh.getClass().getDeclaredField("seqid");
    field.setAccessible(true);
    long oldValue = ((long)field.get(eh));
    field.set(eh, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f185f1cd-01e6-3f78-8b55-254bca496459"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("eaeab3d0-d43c-3e85-b007-5da98c41bec4"))) {
try {
    java.lang.reflect.Field field = eh.getClass().getDeclaredField("waitingTimeForEvents");
    field.setAccessible(true);
    int oldValue = ((int)field.get(eh));
    field.set(eh, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("37394966-e817-35ca-9bb7-c0a7be8483ce"))) {
try {
    java.lang.reflect.Field field = eh.getClass().getDeclaredField("waitingTimeForEvents");
    field.setAccessible(true);
    int oldValue = ((int)field.get(eh));
    field.set(eh, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c5c77d5f-66e2-3efc-8eb0-c0eda5626932"))) {
try {
    java.lang.reflect.Field field = eh.getClass().getDeclaredField("waitingTimeForEvents");
    field.setAccessible(true);
    int oldValue = ((int)field.get(eh));
    field.set(eh, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("97d68e99-1d85-33c6-9a98-a56f79970493"))) {
try {
    java.lang.reflect.Field field = eh.getClass().getDeclaredField("waitingTimeForEvents");
    field.setAccessible(true);
    int oldValue = ((int)field.get(eh));
    field.set(eh, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    Executor executor = getExecutor(eh.getEventType().getExecutorServiceType());
    if (((KnobRuntime.check(java.util.UUID.fromString("44688b34-c8ec-37c8-8aa4-5d456c40ccfc"))) ? ((executor) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("58c03c29-c5de-3f59-8ff5-3953e82e2dc9"))) ? ((executor) != (null)) : (executor == null))))) {
      // This happens only when events are submitted after shutdown() was
      // called, so dropping them should be "ok" since it means we're
      // shutting down.
      LOG.error("Cannot submit [" + eh + "] because the executor is missing."
        + " Is this process shutting down?");
    } else {
if(KnobRuntime.check(java.util.UUID.fromString("7dc14026-7d6b-335a-84d9-85665c931a19"))) {
try {
    java.lang.reflect.Field field = eh.getClass().getDeclaredField("seqid");
    field.setAccessible(true);
    long oldValue = ((long)field.get(eh));
    field.set(eh, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e73ba49f-4fa7-31ac-9bea-8a68415f2396"))) {
try {
    java.lang.reflect.Field field = eh.getClass().getDeclaredField("waitingTimeForEvents");
    field.setAccessible(true);
    int oldValue = ((int)field.get(eh));
    field.set(eh, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("930ad62a-fbf9-31f4-8ee0-43231cca0a31"))) {
try {
    java.lang.reflect.Field field = eh.getClass().getDeclaredField("waitingTimeForEvents");
    field.setAccessible(true);
    int oldValue = ((int)field.get(eh));
    field.set(eh, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("65492739-dc42-34c7-b726-eaeb3a1293de"))) {
try {
    java.lang.reflect.Field field = eh.getClass().getDeclaredField("waitingTimeForEvents");
    field.setAccessible(true);
    int oldValue = ((int)field.get(eh));
    field.set(eh, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e0d29638-e2c0-37e0-a132-151fc7f00d78"))) {
try {
    java.lang.reflect.Field field = eh.getClass().getDeclaredField("waitingTimeForEvents");
    field.setAccessible(true);
    int oldValue = ((int)field.get(eh));
    field.set(eh, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (KnobRuntime.check(java.util.UUID.fromString("a07001f5-bb5e-3584-9169-861468df7b34"))) { submit(eh); } else { executor.submit(eh); }
    }
  }

  // Submit the handler after the given delay. Used for retrying.
  public void delayedSubmit(EventHandler eh, long delay, TimeUnit unit) {
    ListenableFuture<?> future = delayedSubmitTimer.schedule(() -> submit(eh), delay, unit);
    future.addListener(() -> {
      try {
        future.get();
      } catch (Exception e) {
        LOG.error("Failed to submit the event handler {} to executor", eh, e);
      }
    }, MoreExecutors.directExecutor());
  }

  public Map<String, ExecutorStatus> getAllExecutorStatuses() {
    Map<String, ExecutorStatus> ret = Maps.newHashMap();
    for (Map.Entry<String, Executor> e : executorMap.entrySet()) {
      ret.put(e.getKey(), e.getValue().getStatus());
    }
    return ret;
  }

  /**
   * Configuration wrapper for {@link Executor}.
   */
  public class ExecutorConfig {
    // Refer to ThreadPoolExecutor javadoc for details of these configuration.
    // Argument validation and bound checks delegated to the underlying ThreadPoolExecutor
    // implementation.
    public static final long KEEP_ALIVE_TIME_MILLIS_DEFAULT = 1000;
    private int corePoolSize = -1;
    private boolean allowCoreThreadTimeout = false;
    private long keepAliveTimeMillis = KEEP_ALIVE_TIME_MILLIS_DEFAULT;
    private ExecutorType executorType;

    public ExecutorConfig setExecutorType(ExecutorType type) {
if(KnobRuntime.check(java.util.UUID.fromString("d2ca9bad-ac8c-3920-9203-13f8336ab73a"))) {
return null;
}
      this.executorType = type;
      return this;
    }

    private ExecutorType getExecutorType() {
if(KnobRuntime.check(java.util.UUID.fromString("4f6686b0-6fb0-3de1-a09a-7e6b76bc6e61"))) {
return null;
}
      return Preconditions.checkNotNull(executorType, "ExecutorType not set.");
    }

    public int getCorePoolSize() {
if(KnobRuntime.check(java.util.UUID.fromString("5118240a-8cf3-3def-aaec-07e909a5232b"))) {
return 0;
}
      return corePoolSize;
    }

    public ExecutorConfig setCorePoolSize(int corePoolSize) {
if(KnobRuntime.check(java.util.UUID.fromString("1c67f0fc-79f1-3937-817c-588d95b51949"))) {
return null;
}
      this.corePoolSize = corePoolSize;
      return this;
    }

    public boolean allowCoreThreadTimeout() {
if(KnobRuntime.check(java.util.UUID.fromString("23e9c9ca-d6e4-3f69-8c2e-249ef2db0412"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("f8c45b77-1d4f-32b2-b17c-ecd169eefc34"))) {
return false;
}
      return ((KnobRuntime.check(java.util.UUID.fromString("d23d4dfc-950a-382f-935b-04ef70edc330"))) ? (!allowCoreThreadTimeout) : (allowCoreThreadTimeout));
    }

    /**
     * Allows timing out of core threads. Good to set this for non-critical thread pools for release
     * of unused resources. Refer to {@link ThreadPoolExecutor#allowCoreThreadTimeOut} for
     * additional details.
     */
    public ExecutorConfig setAllowCoreThreadTimeout(boolean allowCoreThreadTimeout) {
if(KnobRuntime.check(java.util.UUID.fromString("f42a6603-7542-32cb-888b-106a31aa2c2f"))) {
return null;
}
      this.allowCoreThreadTimeout = allowCoreThreadTimeout;
      return this;
    }

    /**
     * Returns the executor name inferred from the type and the servername on which this is running.
     */
    public String getName() {
if(KnobRuntime.check(java.util.UUID.fromString("eb15244a-5349-39ee-8261-1956abdcd511"))) {
return null;
}
      return ((KnobRuntime.check(java.util.UUID.fromString("06c535a6-9d30-38e6-8580-939cc89b645f"))) ? (getExecutorType().getExecutorName(getExecutorType().getExecutorName(servername))) : (getExecutorType().getExecutorName(servername)));
    }

    public long getKeepAliveTimeMillis() {
if(KnobRuntime.check(java.util.UUID.fromString("8168c27b-5fd4-37ec-a3c7-de28fd9d54ef"))) {
return 0;
}
      return keepAliveTimeMillis;
    }

    public ExecutorConfig setKeepAliveTimeMillis(long keepAliveTimeMillis) {
      this.keepAliveTimeMillis = keepAliveTimeMillis;
      return this;
    }
  }

  /**
   * Executor instance.
   */
  static class Executor {
    // the thread pool executor that services the requests
    final TrackingThreadPoolExecutor threadPoolExecutor;
    // work queue to use - unbounded queue
    final BlockingQueue<Runnable> q = new LinkedBlockingQueue<>();
    private final String name;
    private static final AtomicLong seqids = new AtomicLong(0);
    private final long id;

    protected Executor(ExecutorConfig config) {
      this.id = seqids.incrementAndGet();
      this.name = config.getName();
      // create the thread pool executor
      this.threadPoolExecutor = new TrackingThreadPoolExecutor(
        // setting maxPoolSize > corePoolSize has no effect since we use an unbounded task queue.
        config.getCorePoolSize(), config.getCorePoolSize(), config.getKeepAliveTimeMillis(),
        TimeUnit.MILLISECONDS, q);
      this.threadPoolExecutor.allowCoreThreadTimeOut(config.allowCoreThreadTimeout());
      // name the threads for this threadpool
      ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
      if (KnobRuntime.check(java.util.UUID.fromString("12edfcaf-b7f1-33c6-9d80-d5bdc9ab6e1d"))) { tfb.setNameFormat((this.name) + ("-%d")); } else if (KnobRuntime.check(java.util.UUID.fromString("0511385e-59e6-3311-8ee3-4c2ad20fc4cb"))) { tfb.setNameFormat((this.name + "-%d") + ("-%d")); } else { tfb.setNameFormat(this.name + "-%d"); }
      tfb.setDaemon(true);
      tfb.setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER);
      this.threadPoolExecutor.setThreadFactory(tfb.build());
    }

    /**
     * Submit the event to the queue for handling.
     */
    void submit(final EventHandler event) {
      // If there is a listener for this type, make sure we call the before
      // and after process methods.
if(KnobRuntime.check(java.util.UUID.fromString("58f0c602-be65-3b70-a914-af884020e1df"))) {
try {
    java.lang.reflect.Field field = event.getClass().getDeclaredField("seqid");
    field.setAccessible(true);
    long oldValue = ((long)field.get(event));
    field.set(event, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("34016495-a1f8-348d-8974-01a08acef09c"))) {
try {
    java.lang.reflect.Field field = event.getClass().getDeclaredField("waitingTimeForEvents");
    field.setAccessible(true);
    int oldValue = ((int)field.get(event));
    field.set(event, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("129ed95a-07eb-3158-8603-17e432cca521"))) {
try {
    java.lang.reflect.Field field = event.getClass().getDeclaredField("waitingTimeForEvents");
    field.setAccessible(true);
    int oldValue = ((int)field.get(event));
    field.set(event, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("55e1c42f-8247-37ef-a88f-d234939fc653"))) {
try {
    java.lang.reflect.Field field = event.getClass().getDeclaredField("waitingTimeForEvents");
    field.setAccessible(true);
    int oldValue = ((int)field.get(event));
    field.set(event, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8d355cbf-0f40-3107-a771-b7750ea7c8c4"))) {
try {
    java.lang.reflect.Field field = event.getClass().getDeclaredField("waitingTimeForEvents");
    field.setAccessible(true);
    int oldValue = ((int)field.get(event));
    field.set(event, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3e9f25b3-8e71-3a0c-bb2b-d0ae7850909f"))) {
try {
    java.lang.reflect.Field field = event.getClass().getDeclaredField("seqid");
    field.setAccessible(true);
    long oldValue = ((long)field.get(event));
    field.set(event, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      this.threadPoolExecutor.execute(event);
    }

    TrackingThreadPoolExecutor getThreadPoolExecutor() {
      return threadPoolExecutor;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "-" + id + "-" + name;
    }

    public ExecutorStatus getStatus() {
      List<EventHandler> queuedEvents = Lists.newArrayList();
      for (Runnable r : q) {
        if (!(r instanceof EventHandler)) {
          LOG.warn("Non-EventHandler " + r + " queued in " + name);
          continue;
        }
        queuedEvents.add((EventHandler) r);
      }

      List<RunningEventStatus> running = Lists.newArrayList();
      for (Map.Entry<Thread, Runnable> e : threadPoolExecutor.getRunningTasks().entrySet()) {
        Runnable r = e.getValue();
        if (!(r instanceof EventHandler)) {
          LOG.warn("Non-EventHandler " + r + " running in " + name);
          continue;
        }
        running.add(new RunningEventStatus(e.getKey(), (EventHandler) r));
      }

      return new ExecutorStatus(this, queuedEvents, running);
    }
  }

  /**
   * A subclass of ThreadPoolExecutor that keeps track of the Runnables that are executing at any
   * given point in time.
   */
  static class TrackingThreadPoolExecutor extends ThreadPoolExecutor {
    private ConcurrentMap<Thread, Runnable> running = Maps.newConcurrentMap();

    public TrackingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
      TimeUnit unit, BlockingQueue<Runnable> workQueue) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
if(KnobRuntime.check(java.util.UUID.fromString("5718b97e-b1c3-32db-b535-0728232c3e48"))) {
return;
}
      super.afterExecute(r, t);
      running.remove(Thread.currentThread());
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
if(KnobRuntime.check(java.util.UUID.fromString("aba28a56-4bde-3368-94a5-b385ea7a82cd"))) {
return;
}
      Runnable oldPut = running.put(t, r);
      assert ((KnobRuntime.check(java.util.UUID.fromString("681c65d2-4911-3450-8ed3-9a38baef68ed"))) ? ((oldPut) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fa418d34-0468-3ab1-a9e9-9193996bb4ae"))) ? ((oldPut) != (null)) : (oldPut == null)))) : "inconsistency for thread " + t;
      super.beforeExecute(t, r);
    }

    /**
     * @return a map of the threads currently running tasks inside this executor. Each key is an
     *         active thread, and the value is the task that is currently running. Note that this is
     *         not a stable snapshot of the map.
     */
    public ConcurrentMap<Thread, Runnable> getRunningTasks() {
      return running;
    }
  }

  /**
   * A snapshot of the status of a particular executor. This includes the contents of the executor's
   * pending queue, as well as the threads and events currently being processed. This is a
   * consistent snapshot that is immutable once constructed.
   */
  public static class ExecutorStatus {
    final Executor executor;
    final List<EventHandler> queuedEvents;
    final List<RunningEventStatus> running;

    ExecutorStatus(Executor executor, List<EventHandler> queuedEvents,
      List<RunningEventStatus> running) {
      this.executor = executor;
      this.queuedEvents = queuedEvents;
      this.running = running;
    }

    /**
     * Dump a textual representation of the executor's status to the given writer.
     * @param out    the stream to write to
     * @param indent a string prefix for each line, used for indentation
     */
    public void dumpTo(Writer out, String indent) throws IOException {
      out.write(indent + "Status for executor: " + executor + "\n");
      out.write(indent + "=======================================\n");
      out.write(indent + queuedEvents.size() + " events queued, " + running.size() + " running\n");
      if (!queuedEvents.isEmpty()) {
        out.write(indent + "Queued:\n");
        for (EventHandler e : queuedEvents) {
          out.write(indent + "  " + e + "\n");
        }
        out.write("\n");
      }
      if (!running.isEmpty()) {
        out.write(indent + "Running:\n");
        for (RunningEventStatus stat : running) {
          out.write(indent + "  Running on thread '" + stat.threadInfo.getThreadName() + "': "
            + stat.event + "\n");
          out.write(ThreadMonitoring.formatThreadInfo(stat.threadInfo, indent + "  "));
          out.write("\n");
        }
      }
      out.flush();
    }
  }

  /**
   * The status of a particular event that is in the middle of being handled by an executor.
   */
  public static class RunningEventStatus {
    final ThreadInfo threadInfo;
    final EventHandler event;

    public RunningEventStatus(Thread t, EventHandler event) {
      this.threadInfo = ThreadMonitoring.getThreadInfo(t);
      this.event = event;
    }
  }
}
