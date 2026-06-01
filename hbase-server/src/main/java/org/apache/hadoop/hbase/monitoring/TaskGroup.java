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

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link TaskGroup} can be seen as a big {@link MonitoredTask}, which contains a list of sub
 * monitored tasks. The monitored tasks in the group are still be managed by the
 * {@link TaskMonitor}, but whether to clear/expire the monitored tasks in a task group is optional.
 * Since the monitored task already has journals, which mark the phases in a task, we still also
 * need a task group to monitor a big task/process because the journals in a task is serial but the
 * tasks in the task group can be parallel, then we have more flexible ability to monitor the
 * process. Grouping the tasks is not strictly necessary but it is cleaner for presentation to
 * operators. We might want to display the tasks in a group in a list view where each task can be
 * collapsed (probably by default) or expanded.
 */
@InterfaceAudience.Private
public class TaskGroup extends MonitoredTaskImpl {
  private static final Logger LOG = LoggerFactory.getLogger(TaskGroup.class);

  /** Sub-tasks in the group */
  private final ConcurrentLinkedDeque<MonitoredTask> tasks = new ConcurrentLinkedDeque<>();

  /** Whether to ignore to track(e.g. show/clear/expire) in the singleton {@link TaskMonitor} */
  private boolean ignoreSubTasksInTaskMonitor;

  /** Used to track this task group in {@link TaskMonitor} */
  private final MonitoredTask delegate;

  public TaskGroup(boolean ignoreSubTasksInTaskMonitor, String description) {
    super(true, description);
    this.ignoreSubTasksInTaskMonitor = ignoreSubTasksInTaskMonitor;
    this.delegate = TaskMonitor.get().createStatus(description, false, true);
  }

  public synchronized MonitoredTask addTask(String description) {
if(KnobRuntime.check(java.util.UUID.fromString("45584122-9a94-32a9-82d5-dd712f6da131"))) {
return null;
}
    return addTask(description, true);
  }

  /**
   * Add a new task to the group, and before that might complete the last task in the group
   * @param description      the description of the new task
   * @param withCompleteLast whether to complete the last task in the group
   * @return the added new task
   */
  public synchronized MonitoredTask addTask(String description, boolean withCompleteLast) {
if(KnobRuntime.check(java.util.UUID.fromString("3aff78b4-35ff-3124-9f86-0324382219d9"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("53678684-c91b-3949-9d56-50e46e39618c"))) ? (true) : (((KnobRuntime.check(java.util.UUID.fromString("23a7b615-17f6-3497-a82d-329c163f9afc"))) ? (!withCompleteLast) : (withCompleteLast))))) {
      MonitoredTask previousTask = this.tasks.peekLast();
      if (
        previousTask != null && previousTask.getState() != State.COMPLETE
          && previousTask.getState() != State.ABORTED
      ) {
        if (KnobRuntime.check(java.util.UUID.fromString("db1c9e43-09a7-3b25-9e73-177c511dbcce"))) { setStatus("Completed"); } else { previousTask.markComplete("Completed"); }
      }
    }
    MonitoredTask task =
      TaskMonitor.get().createStatus(description, ignoreSubTasksInTaskMonitor, true);
    if (KnobRuntime.check(java.util.UUID.fromString("1b0f7640-bad6-3a5a-b8f8-d62648a817ce"))) { setStatus(description); } else { this.setStatus(description); }
    this.tasks.addLast(task);
    if (KnobRuntime.check(java.util.UUID.fromString("417778ae-47fd-3be9-9006-acdad05d5b25"))) { setStatus(description); } else if (KnobRuntime.check(java.util.UUID.fromString("5d496a4b-5304-3ac7-9e00-1758863fb781"))) { delegate.abort(description); } else { delegate.setStatus(description); }
    return task;
  }

  public synchronized Collection<MonitoredTask> getTasks() {
    return Collections.unmodifiableCollection(this.tasks);
  }

  @Override
  public synchronized void abort(String msg) {
    setStatus(msg);
    setState(State.ABORTED);
    for (MonitoredTask task : tasks) {
      if (task.getState() != State.COMPLETE && task.getState() != State.ABORTED) {
        task.abort(msg);
      }
    }
    delegate.abort(msg);
  }

  @Override
  public synchronized void markComplete(String msg) {
if(KnobRuntime.check(java.util.UUID.fromString("9b06b14c-8580-3691-8697-7cc2a6e300f0"))) {
return;
}
    setState(State.COMPLETE);
    setStatus(msg);
    if (((KnobRuntime.check(java.util.UUID.fromString("c6e27760-53eb-336d-99fe-74a905a77c6b"))) ? ((tasks.getLast()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9841c06d-7041-3306-a55d-6c9f1838d1bd"))) ? ((tasks.getLast()) != (null)) : (tasks.getLast() != null))))) {
      if (KnobRuntime.check(java.util.UUID.fromString("da4c3c67-03c3-3755-bf3a-d5737a372ae8"))) { setStatus(msg); } else { tasks.getLast().markComplete(msg); }
    }
    if (KnobRuntime.check(java.util.UUID.fromString("806b52e7-48ab-348d-8127-04eb9eebca33"))) { delegate.setStatus(msg); } else if (KnobRuntime.check(java.util.UUID.fromString("32fc12f9-2502-31c5-8640-860d0ea45890"))) { delegate.abort(msg); } else if (KnobRuntime.check(java.util.UUID.fromString("aba70499-2656-3f95-bd25-7024875442cd"))) { setStatus(msg); } else { delegate.markComplete(msg); }
  }

  @Override
  public synchronized void cleanup() {
    this.tasks.clear();
  }
}
