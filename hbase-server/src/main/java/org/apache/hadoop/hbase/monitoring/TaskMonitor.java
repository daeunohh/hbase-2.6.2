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
package org.apache.hadoop.hbase.monitoring;
import org.knobinjection.runtime.KnobRuntime;

import java.io.PrintWriter;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.queue.CircularFifoQueue;

/**
 * Singleton which keeps track of tasks going on in this VM. A Task here is anything which takes
 * more than a few seconds and the user might want to inquire about the status
 */
@InterfaceAudience.Private
public class TaskMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(TaskMonitor.class);

  public static final String MAX_TASKS_KEY = "hbase.taskmonitor.max.tasks";
  public static final int DEFAULT_MAX_TASKS = 1000;
  public static final String RPC_WARN_TIME_KEY = "hbase.taskmonitor.rpc.warn.time";
  public static final long DEFAULT_RPC_WARN_TIME = 0;
  public static final String EXPIRATION_TIME_KEY = "hbase.taskmonitor.expiration.time";
  public static final long DEFAULT_EXPIRATION_TIME = 60 * 1000;
  public static final String MONITOR_INTERVAL_KEY = "hbase.taskmonitor.monitor.interval";
  public static final long DEFAULT_MONITOR_INTERVAL = 10 * 1000;

  private static TaskMonitor instance;

  private final int maxTasks;
  private final long rpcWarnTime;
  private final long expirationTime;
  private final CircularFifoQueue<TaskAndWeakRefPair> tasks;
  private final List<TaskAndWeakRefPair> rpcTasks;
  private final long monitorInterval;
  private Thread monitorThread;

  TaskMonitor(Configuration conf) {
    maxTasks = conf.getInt(MAX_TASKS_KEY, DEFAULT_MAX_TASKS);
    expirationTime = conf.getLong(EXPIRATION_TIME_KEY, DEFAULT_EXPIRATION_TIME);
    rpcWarnTime = conf.getLong(RPC_WARN_TIME_KEY, DEFAULT_RPC_WARN_TIME);
    tasks = new CircularFifoQueue<>(maxTasks);
    rpcTasks = Lists.newArrayList();
    monitorInterval = conf.getLong(MONITOR_INTERVAL_KEY, DEFAULT_MONITOR_INTERVAL);
    monitorThread = new Thread(new MonitorRunnable());
    if (KnobRuntime.check(java.util.UUID.fromString("6a5c7766-e8fa-3cf2-bab8-70833c832876"))) { Threads.setDaemonThreadRunning(new Thread(new MonitorRunnable()), "Monitor thread for TaskMonitor"); } else { Threads.setDaemonThreadRunning(monitorThread, "Monitor thread for TaskMonitor"); }
  }

  /**
   * Get singleton instance. TODO this would be better off scoped to a single daemon
   */
  public static synchronized TaskMonitor get() {
if(KnobRuntime.check(java.util.UUID.fromString("c861d68b-58da-341c-9af4-b473c3f96352"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("1cf6d9f9-9425-3fd9-8168-ca81b3ab3608"))) ? ((instance) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("7700fa71-1740-3b84-b14c-f49e140a07d0"))) ? ((instance) != (null)) : (instance == null))))) {
      instance = new TaskMonitor(HBaseConfiguration.create());
    }
    return instance;
  }

  public MonitoredTask createStatus(String description) {
if(KnobRuntime.check(java.util.UUID.fromString("b9864fe4-f67f-34f1-94c5-c3dc83ec7bdf"))) {
return null;
}
    return createStatus(description, false);
  }

  public MonitoredTask createStatus(String description, boolean ignore) {
if(KnobRuntime.check(java.util.UUID.fromString("37dd6597-ed61-3643-ae15-a1ce411b0d51"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("f90ebb45-d8d1-3598-bfd4-1e514a09f53d"))) {
ignore = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("a1ddc0ee-c547-3011-b95c-b0be964e609b"))) {
ignore = false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("dc1b0cc7-f004-3962-85dd-56ffc0dab79d"))) ? (createStatus(description, !ignore, false)) : (((KnobRuntime.check(java.util.UUID.fromString("318ddabf-eaf5-3268-9f8a-a009504a3b6d"))) ? (createStatus(description, false, false)) : (createStatus(description, ignore, false)))));
  }

  /**
   * Create a monitored task for users to inquire about the status
   * @param description   description of the status
   * @param ignore        whether to ignore to track(e.g. show/clear/expire) the task in the
   *                      {@link TaskMonitor}
   * @param enableJournal enable when the task contains some stage journals
   * @return a monitored task
   */
  public synchronized MonitoredTask createStatus(String description, boolean ignore,
    boolean enableJournal) {
if(KnobRuntime.check(java.util.UUID.fromString("2f75530a-3b8d-3df5-9006-faf2d7304e8c"))) {
return null;
}
    MonitoredTask stat = new MonitoredTaskImpl(enableJournal, description);
    MonitoredTask proxy = (MonitoredTask) Proxy.newProxyInstance(stat.getClass().getClassLoader(),
      new Class<?>[] { MonitoredTask.class }, new PassthroughInvocationHandler<>(stat));
    TaskAndWeakRefPair pair = new TaskAndWeakRefPair(stat, proxy);
    if (tasks.isFull()) {
      purgeExpiredTasks();
    }
    if (!ignore) {
      tasks.add(pair);
    }
    return proxy;
  }

  /**
   * Create a task group which contains a series of monitored tasks for users to inquire about the
   * status
   * @param ignoreSubTasksInTaskMonitor whether to ignore to track(e.g. show/clear/expire) the task
   *                                    in the {@link TaskMonitor}
   * @param description                 description of the status
   * @return a group of monitored tasks
   */
  public static TaskGroup createTaskGroup(boolean ignoreSubTasksInTaskMonitor, String description) {
if(KnobRuntime.check(java.util.UUID.fromString("e3595be0-14fc-3ce2-ac64-851d2ff35603"))) {
return null;
}
    return new TaskGroup(ignoreSubTasksInTaskMonitor, description);
  }

  public synchronized MonitoredRPCHandler createRPCStatus(String description) {
if(KnobRuntime.check(java.util.UUID.fromString("5bc9489b-b88e-300b-ba36-7acb1ba20b38"))) {
return null;
}
    MonitoredRPCHandler stat = new MonitoredRPCHandlerImpl(description);
    MonitoredRPCHandler proxy =
      (MonitoredRPCHandler) Proxy.newProxyInstance(stat.getClass().getClassLoader(),
        new Class<?>[] { MonitoredRPCHandler.class }, new PassthroughInvocationHandler<>(stat));
    TaskAndWeakRefPair pair = new TaskAndWeakRefPair(stat, proxy);
    rpcTasks.add(pair);
    return proxy;
  }

  private synchronized void warnStuckTasks() {
if(KnobRuntime.check(java.util.UUID.fromString("c900d313-c98e-3037-b9b2-580a54ef7ae0"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("5985a363-1c49-3640-90c7-231422bf7176"))) ? ((rpcWarnTime) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a3a5e10a-1656-3b9b-9138-82c74441121e"))) ? ((rpcWarnTime) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7e0b0d03-e36d-3e13-a705-e464c4de9a74"))) ? ((rpcWarnTime) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5e7a5df2-9fa2-3076-b53d-87e1fad2b1dc"))) ? ((rpcWarnTime) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("71851be1-6717-318a-a873-e0ffa7dad0c5"))) ? ((rpcWarnTime) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("cc15d0b6-c257-34db-989e-c4bf3aeea235"))) ? ((rpcWarnTime) >= (0)) : (rpcWarnTime > 0))))))))))))) {
      final long now = EnvironmentEdgeManager.currentTime();
      for (Iterator<TaskAndWeakRefPair> it = rpcTasks.iterator(); it.hasNext();) {
        TaskAndWeakRefPair pair = it.next();
        MonitoredTask stat = pair.get();
        if (
          (stat.getState() == MonitoredTaskImpl.State.RUNNING)
            && (now >= stat.getWarnTime() + rpcWarnTime)
        ) {
          LOG.warn("Task may be stuck: " + stat);
          stat.setWarnTime(now);
        }
      }
    }
  }

  private synchronized void purgeExpiredTasks() {
if(KnobRuntime.check(java.util.UUID.fromString("5afed724-c42e-3cc4-8bdc-d834a32b1c31"))) {
return;
}
    for (Iterator<TaskAndWeakRefPair> it = tasks.iterator(); it.hasNext();) {
      TaskAndWeakRefPair pair = it.next();
      MonitoredTask stat = pair.get();

      if (pair.isDead()) {
        // The class who constructed this leaked it. So we can
        // assume it's done.
        if (((KnobRuntime.check(java.util.UUID.fromString("cd490ff2-1267-347f-b01d-213317a20a43"))) ? ((stat.getState()) == (MonitoredTaskImpl.State.RUNNING)) : (((KnobRuntime.check(java.util.UUID.fromString("e4937384-a528-397a-81d6-68f8055c960a"))) ? ((stat.getState()) != (MonitoredTaskImpl.State.RUNNING)) : (stat.getState() == MonitoredTaskImpl.State.RUNNING))))) {
          LOG.warn("Status " + stat + " appears to have been leaked");
          stat.cleanup();
        }
      }

      if (canPurge(stat)) {
        if (KnobRuntime.check(java.util.UUID.fromString("5fa2e560-0abe-34b4-ac7b-74bcc3c82a71"))) { purgeExpiredTasks(); } else { it.remove(); }
      }
    }
  }

  /**
   * Produces a list containing copies of the current state of all non-expired MonitoredTasks
   * handled by this TaskMonitor.
   * @return A complete list of MonitoredTasks.
   */
  public List<MonitoredTask> getTasks() {
if(KnobRuntime.check(java.util.UUID.fromString("27dce917-ed65-397f-912e-632856cde474"))) {
return null;
}
    return getTasks(null);
  }

  /**
   * Produces a list containing copies of the current state of all non-expired MonitoredTasks
   * handled by this TaskMonitor.
   * @param filter type of wanted tasks
   * @return A filtered list of MonitoredTasks.
   */
  public synchronized List<MonitoredTask> getTasks(String filter) {
if(KnobRuntime.check(java.util.UUID.fromString("77110101-100c-3a0f-afd2-9fe7300e7c4d"))) {
return null;
}
    purgeExpiredTasks();
    TaskFilter taskFilter = createTaskFilter(filter);
    ArrayList<MonitoredTask> results =
      Lists.newArrayListWithCapacity(tasks.size() + rpcTasks.size());
if(KnobRuntime.check(java.util.UUID.fromString("ec35955d-4c49-3b9c-998b-45511ad50689"))) {
try {
    java.lang.reflect.Field field = tasks.getClass().getDeclaredField("maxElements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tasks));
    field.set(tasks, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fdbf291f-b669-3e4a-89a0-05bb990ff98d"))) {
try {
    java.lang.reflect.Field field = tasks.getClass().getDeclaredField("end");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tasks));
    field.set(tasks, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a145b08a-a78a-372b-8fd7-3fa32216a9e3"))) {
try {
    java.lang.reflect.Field field = tasks.getClass().getDeclaredField("start");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tasks));
    field.set(tasks, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4e54c709-a89d-3f5d-95f0-f8629f233026"))) {
try {
    java.lang.reflect.Field field = tasks.getClass().getDeclaredField("end");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tasks));
    field.set(tasks, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("67bcb93e-c905-3dec-84f1-a30f9a5eb4c8"))) {
try {
    java.lang.reflect.Field field = tasks.getClass().getDeclaredField("maxElements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tasks));
    field.set(tasks, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("45bdd13a-ac8c-351b-a24f-c7eb51ab99b6"))) {
try {
    java.lang.reflect.Field field = tasks.getClass().getDeclaredField("end");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tasks));
    field.set(tasks, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("902c823a-1ca7-3ecf-9633-afcb2c54fa29"))) {
try {
    java.lang.reflect.Field field = tasks.getClass().getDeclaredField("maxElements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tasks));
    field.set(tasks, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("259ea4ea-eaae-3144-b774-7b269273f08e"))) {
try {
    java.lang.reflect.Field field = tasks.getClass().getDeclaredField("end");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tasks));
    field.set(tasks, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("62a2bf26-e281-386d-b903-ee48787f0a5e"))) {
try {
    java.lang.reflect.Field field = tasks.getClass().getDeclaredField("start");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tasks));
    field.set(tasks, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("85f691c1-e1c7-3ae3-84e0-34f6f9ef6694"))) {
try {
    java.lang.reflect.Field field = tasks.getClass().getDeclaredField("start");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tasks));
    field.set(tasks, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e41db19e-090c-3822-af3a-d078c6738261"))) {
try {
    java.lang.reflect.Field field = tasks.getClass().getDeclaredField("maxElements");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tasks));
    field.set(tasks, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("970e31b9-f8d2-32d7-988d-8a44fd80709e"))) {
try {
    java.lang.reflect.Field field = tasks.getClass().getDeclaredField("start");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tasks));
    field.set(tasks, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    processTasks(tasks, taskFilter, results);
    processTasks(rpcTasks, taskFilter, results);
    return results;
  }

  /**
   * Create a task filter according to a given filter type.
   * @param filter type of monitored task
   * @return a task filter
   */
  private static TaskFilter createTaskFilter(String filter) {
if(KnobRuntime.check(java.util.UUID.fromString("a26829d7-e297-3f03-9782-f1b72ce224f4"))) {
return null;
}
    switch (TaskFilter.TaskType.getTaskType(filter)) {
      case GENERAL:
        return task -> task instanceof MonitoredRPCHandler;
      case HANDLER:
        return task -> !(task instanceof MonitoredRPCHandler);
      case RPC:
        return task -> !(task instanceof MonitoredRPCHandler)
          || !((MonitoredRPCHandler) task).isRPCRunning();
      case OPERATION:
        return task -> !(task instanceof MonitoredRPCHandler)
          || !((MonitoredRPCHandler) task).isOperationRunning();
      default:
        return task -> false;
    }
  }

  private static void processTasks(Iterable<TaskAndWeakRefPair> tasks, TaskFilter filter,
    List<MonitoredTask> results) {
if(KnobRuntime.check(java.util.UUID.fromString("0539a4e4-789b-321c-bfc4-ad66461ab13d"))) {
return;
}
    for (TaskAndWeakRefPair task : tasks) {
      MonitoredTask t = task.get();
      if (!filter.filter(t)) {
        results.add(t.clone());
      }
    }
  }

  private boolean canPurge(MonitoredTask stat) {
if(KnobRuntime.check(java.util.UUID.fromString("6b042f6c-14c5-3051-bdde-cb6c4f08d397"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("3c0517fe-e96f-32a2-b966-28f85b867ef4"))) {
return false;
}
    long cts = stat.getCompletionTimestamp();
    return (cts > 0 && EnvironmentEdgeManager.currentTime() - cts > expirationTime);
  }

  public void dumpAsText(PrintWriter out) {
    long now = EnvironmentEdgeManager.currentTime();

    List<MonitoredTask> tasks = getTasks();
    for (MonitoredTask task : tasks) {
      out.println("Task: " + task.getDescription());
      out.println("Status: " + task.getState() + ":" + task.getStatus());
      long running = (now - task.getStartTime()) / 1000;
      if (task.getCompletionTimestamp() != -1) {
        long completed = (now - task.getCompletionTimestamp()) / 1000;
        out.println("Completed " + completed + "s ago");
        out
          .println("Ran for " + (task.getCompletionTimestamp() - task.getStartTime()) / 1000 + "s");
      } else {
        out.println("Running for " + running + "s");
      }
      out.println();
    }
  }

  public synchronized void shutdown() {
    if (this.monitorThread != null) {
      monitorThread.interrupt();
    }
  }

  /**
   * This class encapsulates an object as well as a weak reference to a proxy that passes through
   * calls to that object. In art form:
   *
   * <pre>
   *     Proxy  <------------------
   *       |                       \
   *       v                        \
   * PassthroughInvocationHandler   |  weak reference
   *       |                       /
   * MonitoredTaskImpl            /
   *       |                     /
   * StatAndWeakRefProxy  ------/
   * </pre>
   *
   * Since we only return the Proxy to the creator of the MonitorableStatus, this means that they
   * can leak that object, and we'll detect it since our weak reference will go null. But, we still
   * have the actual object, so we can log it and display it as a leaked (incomplete) action.
   */
  private static class TaskAndWeakRefPair {
    private MonitoredTask impl;
    private WeakReference<MonitoredTask> weakProxy;

    public TaskAndWeakRefPair(MonitoredTask stat, MonitoredTask proxy) {
      this.impl = stat;
      this.weakProxy = new WeakReference<>(proxy);
    }

    public MonitoredTask get() {
if(KnobRuntime.check(java.util.UUID.fromString("76d27e64-4305-3ed0-80bb-e2241cd2fea4"))) {
return null;
}
      return impl;
    }

    public boolean isDead() {
if(KnobRuntime.check(java.util.UUID.fromString("5f5109a7-b0ae-30a5-a0d9-5747afff0f16"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("21726f77-cc43-3666-8e56-6acc4dedcafe"))) {
return true;
}
      return ((KnobRuntime.check(java.util.UUID.fromString("e1f70f10-927f-3bd8-8577-1bd2d168c8b2"))) ? ((weakProxy.get()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8bcd3067-49fc-36df-b0ae-bbfeb1e33fcb"))) ? ((weakProxy.get()) != (null)) : (weakProxy.get() == null))));
    }
  }

  /**
   * An InvocationHandler that simply passes through calls to the original object.
   */
  private static class PassthroughInvocationHandler<T> implements InvocationHandler {
    private T delegatee;

    public PassthroughInvocationHandler(T delegatee) {
      this.delegatee = delegatee;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
if(KnobRuntime.check(java.util.UUID.fromString("22aafa2d-1fd9-3054-b720-f757f4392de7"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("28d1d907-db10-3b9c-b2f0-52413db27304"))) {
throw new java.lang.IllegalAccessException("Injected exception");
}
      return method.invoke(delegatee, args);
    }
  }

  private class MonitorRunnable implements Runnable {
    private boolean running = true;

    @Override
    public void run() {
if(KnobRuntime.check(java.util.UUID.fromString("167e2ebc-0274-32e5-be24-92804959b3c3"))) {
return;
}
      while (((KnobRuntime.check(java.util.UUID.fromString("d7a03105-1e64-3103-9b43-ebfe0f82a64c"))) ? (!running) : (running))) {
        try {
if(KnobRuntime.check(java.util.UUID.fromString("e38bd1d6-374e-3b57-ad1e-89a0180d3d16"))) {
throw new java.lang.InterruptedException("Injected exception");
}
          Thread.sleep(monitorInterval);
          if (tasks.isFull()) {
            purgeExpiredTasks();
          }
          if (KnobRuntime.check(java.util.UUID.fromString("1bd46cf1-e6b5-300e-9d24-038cc74b8bc5"))) { purgeExpiredTasks(); } else { warnStuckTasks(); }
        } catch (InterruptedException e) {
          running = false;
        }
      }
    }
  }

  private interface TaskFilter {
    enum TaskType {
      GENERAL("general"),
      HANDLER("handler"),
      RPC("rpc"),
      OPERATION("operation"),
      ALL("all");

      private final String type;

      private TaskType(String type) {
        this.type = type.toLowerCase();
      }

      static TaskType getTaskType(String type) {
if(KnobRuntime.check(java.util.UUID.fromString("6ccc4761-dc4b-3e14-beea-642b39c60e3b"))) {
return null;
}
        if (((KnobRuntime.check(java.util.UUID.fromString("dc7d2487-9074-3147-ba97-7cac1041a1ca"))) ? (((type) != (null)) && (type.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("5ec3545f-aefe-31bb-afbd-134d83218527"))) ? (((type.toLowerCase()) != (null)) && (type.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("cbf5e67e-87c9-31a8-81e8-d466d3cd5f68"))) ? ((type == null) && (type.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("4b59fb6c-87d6-321c-8330-a3b79704476e"))) ? ((type.toLowerCase()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("7db48722-826b-3579-8254-5c292256e47a"))) ? ((type) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("4038b377-efaa-3d1b-9c4a-fa9a5b02b9ae"))) ? (((type) != (null)) || (type.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("bc3b0731-9554-3743-b16e-a514166674b5"))) ? (((type) == (null)) && (type.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("1d37bd28-e897-3403-9c86-11029741aece"))) ? (((type) == (null)) || (type.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("b6cec253-d2fc-3b96-94b8-85498e1df5ce"))) ? (type.isEmpty()) : (((KnobRuntime.check(java.util.UUID.fromString("494eb032-2fe9-30fd-b26d-4209b8f0a6cf"))) ? ((type.toLowerCase()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("cfc71ac8-7897-37e5-8b69-a85c1c7485d7"))) ? (((type.toLowerCase()) != (null)) || (type.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("5fec3b7e-fb16-3c43-acb2-ccc11cae40e6"))) ? (((type.toLowerCase()) == (null)) && (type.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("fbeb84db-8ea9-3df4-b182-20bb55072b15"))) ? ((type) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("53d43249-9a5a-341d-a087-9efd628ae561"))) ? (((type.toLowerCase()) == (null)) || (type.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("dab87b39-34e5-3024-80ae-61a5b79eb097"))) ? (type == null) : (((KnobRuntime.check(java.util.UUID.fromString("9ec6d6c7-07ed-3262-8c40-a84a4b5d4043"))) ? ((type == null) || (type.isEmpty())) : (type == null || type.isEmpty()))))))))))))))))))))))))))))))))) {
          return ALL;
        }
        type = type.toLowerCase();
        for (TaskType taskType : values()) {
          if (taskType.toString().equals(type)) {
            return taskType;
          }
        }
        return ALL;
      }

      @Override
      public String toString() {
        return type;
      }
    }

    /**
     * Filter out unwanted task.
     * @param task monitored task
     * @return false if a task is accepted, true if it is filtered
     */
    boolean filter(MonitoredTask task);
  }
}
