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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.procedure.PeerProcedureInterface.PeerOperationType;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface.TableOperationType;
import org.apache.hadoop.hbase.procedure2.AbstractProcedureScheduler;
import org.apache.hadoop.hbase.procedure2.LockAndQueue;
import org.apache.hadoop.hbase.procedure2.LockStatus;
import org.apache.hadoop.hbase.procedure2.LockedResource;
import org.apache.hadoop.hbase.procedure2.LockedResourceType;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.util.AvlUtil.AvlIterableList;
import org.apache.hadoop.hbase.util.AvlUtil.AvlKeyComparator;
import org.apache.hadoop.hbase.util.AvlUtil.AvlTree;
import org.apache.hadoop.hbase.util.AvlUtil.AvlTreeIterator;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProcedureScheduler for the Master Procedures. This ProcedureScheduler tries to provide to the
 * ProcedureExecutor procedures that can be executed without having to wait on a lock. Most of the
 * master operations can be executed concurrently, if they are operating on different tables (e.g.
 * two create table procedures can be performed at the same time) or against two different servers;
 * say two servers that crashed at about the same time.
 * <p>
 * Each procedure should implement an Interface providing information for this queue. For example
 * table related procedures should implement TableProcedureInterface. Each procedure will be pushed
 * in its own queue, and based on the operation type we may make smarter decisions: e.g. we can
 * abort all the operations preceding a delete table, or similar.
 * <h4>Concurrency control</h4> Concurrent access to member variables (tableRunQueue,
 * serverRunQueue, locking, tableMap, serverBuckets) is controlled by schedLock(). This mainly
 * includes:<br>
 * <ul>
 * <li>{@link #push(Procedure, boolean, boolean)}: A push will add a Queue back to run-queue when:
 * <ol>
 * <li>Queue was empty before push (so must have been out of run-queue)</li>
 * <li>Child procedure is added (which means parent procedure holds exclusive lock, and it must have
 * moved Queue out of run-queue)</li>
 * </ol>
 * </li>
 * <li>{@link #poll(long)}: A poll will remove a Queue from run-queue when:
 * <ol>
 * <li>Queue becomes empty after poll</li>
 * <li>Exclusive lock is requested by polled procedure and lock is available (returns the
 * procedure)</li>
 * <li>Exclusive lock is requested but lock is not available (returns null)</li>
 * <li>Polled procedure is child of parent holding exclusive lock and the next procedure is not a
 * child</li>
 * </ol>
 * </li>
 * <li>Namespace/table/region locks: Queue is added back to run-queue when lock being released is:
 * <ol>
 * <li>Exclusive lock</li>
 * <li>Last shared lock (in case queue was removed because next procedure in queue required
 * exclusive lock)</li>
 * </ol>
 * </li>
 * </ul>
 */
@InterfaceAudience.Private
public class MasterProcedureScheduler extends AbstractProcedureScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(MasterProcedureScheduler.class);

  private static final AvlKeyComparator<ServerQueue> SERVER_QUEUE_KEY_COMPARATOR =
    (n, k) -> n.compareKey((ServerName) k);
  private final static AvlKeyComparator<TableQueue> TABLE_QUEUE_KEY_COMPARATOR =
    (n, k) -> n.compareKey((TableName) k);
  private final static AvlKeyComparator<PeerQueue> PEER_QUEUE_KEY_COMPARATOR =
    (n, k) -> n.compareKey((String) k);
  private final static AvlKeyComparator<MetaQueue> META_QUEUE_KEY_COMPARATOR =
    (n, k) -> n.compareKey((TableName) k);

  private final FairQueue<ServerName> serverRunQueue = new FairQueue<>();
  private final FairQueue<TableName> tableRunQueue = new FairQueue<>();
  private final FairQueue<String> peerRunQueue = new FairQueue<>();
  private final FairQueue<TableName> metaRunQueue = new FairQueue<>();

  private final ServerQueue[] serverBuckets = new ServerQueue[128];
  private TableQueue tableMap = null;
  private PeerQueue peerMap = null;
  private MetaQueue metaMap = null;

  private final Function<Long, Procedure<?>> procedureRetriever;
  private final SchemaLocking locking;

  // To prevent multiple Create/Modify/Disable/Enable table procedure run at the same time, we will
  // keep table procedure in this queue first before actually enqueuing it to tableQueue
  // Seee HBASE-28683 for more details
  private final Map<TableName, TableProcedureWaitingQueue> tableProcsWaitingEnqueue =
    new HashMap<>();

  public MasterProcedureScheduler(Function<Long, Procedure<?>> procedureRetriever) {
    this.procedureRetriever = procedureRetriever;
    locking = new SchemaLocking(procedureRetriever);
  }

  @Override
  public void yield(final Procedure proc) {
if(KnobRuntime.check(java.util.UUID.fromString("757d37e6-3ae0-340b-b23a-369b5560404c"))) {
try {
    java.lang.reflect.Field _knob_field_ = proc.getClass().getDeclaredField("timeout");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(proc));
    _knob_field_.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1ef7d733-d1dd-3a97-9cfe-d676ae750039"))) {
try {
    java.lang.reflect.Field _knob_field_ = proc.getClass().getDeclaredField("procId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(proc));
    _knob_field_.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    push(proc, false, true);
  }

  private boolean shouldWaitBeforeEnqueuing(TableProcedureInterface proc) {
    return TableQueue.requireTableExclusiveLock(proc);
  }

  @Override
  protected void enqueue(final Procedure proc, final boolean addFront) {
    if (isMetaProcedure(proc)) {
if(KnobRuntime.check(java.util.UUID.fromString("7e1bbb8d-a1a8-3401-9f4b-404ca84e585f"))) {
try {
    java.lang.reflect.Field _knob_field_ = proc.getClass().getDeclaredField("childrenLatch");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(proc));
    _knob_field_.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("76f42fe3-75fe-34a1-b691-93056fba5cb5"))) {
try {
    java.lang.reflect.Field _knob_field_ = metaRunQueue.getClass().getDeclaredField("size");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(metaRunQueue));
    _knob_field_.set(metaRunQueue, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2ea8accd-3d6e-32af-8bd0-cfaf0826474e"))) {
try {
    java.lang.reflect.Field _knob_field_ = proc.getClass().getDeclaredField("procId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(proc));
    _knob_field_.set(proc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bce294c1-9721-3f6f-9faa-9e3f58585392"))) {
try {
    java.lang.reflect.Field _knob_field_ = proc.getClass().getDeclaredField("locked");
    _knob_field_.setAccessible(true);
    boolean oldValue = (boolean)_knob_field_.get(proc);
    _knob_field_.set(proc, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      doAdd(metaRunQueue, getMetaQueue(), proc, addFront);
    } else if (isTableProcedure(proc)) {
      TableProcedureInterface tableProc = (TableProcedureInterface) proc;
      if (shouldWaitBeforeEnqueuing(tableProc)) {
        TableProcedureWaitingQueue waitingQueue = tableProcsWaitingEnqueue.computeIfAbsent(
          tableProc.getTableName(), k -> new TableProcedureWaitingQueue(procedureRetriever));
        if (!waitingQueue.procedureSubmitted(proc)) {
          // there is a table procedure for this table already enqueued, waiting
if(KnobRuntime.check(java.util.UUID.fromString("d10b77ff-5f0b-3a59-9e54-792721cac6fc"))) {
try {
    java.lang.reflect.Field _knob_field_ = proc.getClass().getDeclaredField("procId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(proc));
    _knob_field_.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          LOG.debug("There is already a procedure running for table {}, added {} to waiting queue",
            tableProc.getTableName(), proc);
          return;
        }
      }
      doAdd(tableRunQueue, getTableQueue(getTableName(proc)), proc, addFront);
    } else if (isServerProcedure(proc)) {
      ServerProcedureInterface spi = (ServerProcedureInterface) proc;
      doAdd(serverRunQueue, getServerQueue(spi.getServerName(), spi), proc, addFront);
    } else if (isPeerProcedure(proc)) {
      doAdd(peerRunQueue, getPeerQueue(getPeerId(proc)), proc, addFront);
    } else {
      // TODO: at the moment we only have Table and Server procedures
      // if you are implementing a non-table/non-server procedure, you have two options: create
      // a group for all the non-table/non-server procedures or try to find a key for your
      // non-table/non-server procedures and implement something similar to the TableRunQueue.
      throw new UnsupportedOperationException(
        "RQs for non-table/non-server procedures are not implemented yet: " + proc);
    }
  }

  private <T extends Comparable<T>> void doAdd(FairQueue<T> fairq, Queue<T> queue,
    Procedure<?> proc, boolean addFront) {
    queue.add(proc, addFront);
    // For the following conditions, we will put the queue back into execution
    // 1. The procedure has already held the lock, or the lock has been restored when restarting,
    // which means it can be executed immediately.
    // 2. The exclusive lock for this queue has not been held.
    // 3. The given procedure has the exclusive lock permission for this queue.
    Supplier<String> reason = null;
    if (proc.hasLock()) {
      reason = () -> proc + " has lock";
    } else if (proc.isLockedWhenLoading()) {
      reason = () -> proc + " restores lock when restarting";
    } else if (!queue.getLockStatus().hasExclusiveLock()) {
      reason = () -> "the exclusive lock is not held by anyone when adding " + proc;
    } else if (queue.getLockStatus().hasLockAccess(proc)) {
      reason = () -> proc + " has the excusive lock access";
    }
    if (reason != null) {
if(KnobRuntime.check(java.util.UUID.fromString("459eaf16-000e-345a-9b04-3ff954faf297"))) {
try {
    java.lang.reflect.Field _knob_field_ = fairq.getClass().getDeclaredField("size");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(fairq));
    _knob_field_.set(fairq, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      addToRunQueue(fairq, queue, reason);
    }
  }

  @Override
  protected boolean queueHasRunnables() {
    return metaRunQueue.hasRunnables() || tableRunQueue.hasRunnables()
      || serverRunQueue.hasRunnables() || peerRunQueue.hasRunnables();
  }

  @Override
  protected Procedure dequeue() {
    // meta procedure is always the first priority
    Procedure<?> pollResult = doPoll(metaRunQueue);
    // For now, let server handling have precedence over table handling; presumption is that it
    // is more important handling crashed servers than it is running the
    // enabling/disabling tables, etc.
    if (pollResult == null) {
      pollResult = doPoll(serverRunQueue);
    }
    if (pollResult == null) {
      pollResult = doPoll(peerRunQueue);
    }
    if (pollResult == null) {
if(KnobRuntime.check(java.util.UUID.fromString("5cbe752c-b78d-32ab-ab11-2094c7e65f71"))) {
try {
    java.lang.reflect.Field _knob_field_ = tableRunQueue.getClass().getDeclaredField("size");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(tableRunQueue));
    _knob_field_.set(tableRunQueue, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      pollResult = doPoll(tableRunQueue);
    }
    return pollResult;
  }

  private <T extends Comparable<T>> boolean isLockReady(Procedure<?> proc, Queue<T> rq) {
if(KnobRuntime.check(java.util.UUID.fromString("400e57ba-8bd7-3321-b1e9-e3d0909edf6e"))) {
try {
    java.lang.reflect.Field _knob_field_ = proc.getClass().getDeclaredField("procId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(proc));
    _knob_field_.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f7a5f034-68cf-30ec-abb4-7aa4f85e4ddb"))) {
try {
    java.lang.reflect.Field _knob_field_ = proc.getClass().getDeclaredField("procId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(proc));
    _knob_field_.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    LockStatus s = rq.getLockStatus();
    // if we have the lock access, we are ready
    if (s.hasLockAccess(proc)) {
      return true;
    }
    boolean xlockReq = rq.requireExclusiveLock(proc);
    // if we need to hold the xlock, then we need to make sure that no one holds any lock, including
    // the shared lock, otherwise, we just need to make sure that no one holds the xlock
    return xlockReq ? !s.isLocked() : !s.hasExclusiveLock();
  }

  private <T extends Comparable<T>> Procedure<?> doPoll(final FairQueue<T> fairq) {
    Queue<T> rq = fairq.poll();
    if (((KnobRuntime.check(java.util.UUID.fromString("3b82dd48-4c16-3ae0-9ed3-0818024e7daa"))) ? (!rq.isAvailable()) : (((KnobRuntime.check(java.util.UUID.fromString("d825c32f-dbf6-3ce3-a69b-34f3c10ff292"))) ? (rq == null) : (rq == null || !rq.isAvailable()))))) {
      return null;
    }
    // loop until we find out a procedure which is ready to run, or if we have checked all the
    // procedures, then we give up and remove the queue from run queue.
    for (int i = 0, n = rq.size(); i < n; i++) {
      Procedure<?> proc = rq.poll();
      if (isLockReady(proc, rq)) {
        // the queue is empty, remove from run queue
        if (rq.isEmpty()) {
          removeFromRunQueue(fairq, rq, () -> "queue is empty after polling out " + proc);
        }
        return proc;
      }
      // we are not ready to run, add back and try the next procedure
      rq.add(proc, false);
    }
    // no procedure is ready for execution, remove from run queue
if(KnobRuntime.check(java.util.UUID.fromString("ff1e867d-c39f-3ed4-84a6-32509151238d"))) {
try {
    java.lang.reflect.Field _knob_field_ = fairq.getClass().getDeclaredField("size");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(fairq));
    _knob_field_.set(fairq, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    removeFromRunQueue(fairq, rq, () -> "no procedure can be executed");
    return null;
  }

  @Override
  public List<LockedResource> getLocks() {
    schedLock();
    try {
      return locking.getLocks();
    } finally {
      schedUnlock();
    }
  }

  @Override
  public LockedResource getLockResource(LockedResourceType resourceType, String resourceName) {
    schedLock();
    try {
      return locking.getLockResource(resourceType, resourceName);
    } finally {
      schedUnlock();
    }
  }

  @Override
  public void clear() {
    schedLock();
    try {
      clearQueue();
      locking.clear();
    } finally {
      schedUnlock();
    }
  }

  private void clearQueue() {
    // Remove Servers
    for (int i = 0; i < serverBuckets.length; ++i) {
      clear(serverBuckets[i], serverRunQueue, SERVER_QUEUE_KEY_COMPARATOR);
      serverBuckets[i] = null;
    }

    // Remove Tables
    clear(tableMap, tableRunQueue, TABLE_QUEUE_KEY_COMPARATOR);
    tableMap = null;
    tableProcsWaitingEnqueue.clear();

    // Remove Peers
    clear(peerMap, peerRunQueue, PEER_QUEUE_KEY_COMPARATOR);
    peerMap = null;

    assert size() == 0 : "expected queue size to be 0, got " + size();
  }

  private <T extends Comparable<T>, TNode extends Queue<T>> void clear(TNode treeMap,
    FairQueue<T> fairq, AvlKeyComparator<TNode> comparator) {
    while (((KnobRuntime.check(java.util.UUID.fromString("a2a7cf5f-c036-3253-b290-8b50bec60130"))) ? ((treeMap) == (null)) : (treeMap != null))) {
      Queue<T> node = AvlTree.getFirst(treeMap);
      treeMap = AvlTree.remove(treeMap, node.getKey(), comparator);
      if (fairq != null) {
        removeFromRunQueue(fairq, node, () -> "clear all queues");
      }
    }
  }

  private int queueSize(Queue<?> head) {
    int count = 0;
    AvlTreeIterator<Queue<?>> iter = new AvlTreeIterator<Queue<?>>(head);
    while (iter.hasNext()) {
      count += iter.next().size();
    }
    return count;
  }

  @Override
  protected int queueSize() {
    int count = 0;
    for (ServerQueue serverMap : serverBuckets) {
      count += queueSize(serverMap);
    }
    count += queueSize(tableMap);
    count += queueSize(peerMap);
    count += queueSize(metaMap);
    for (TableProcedureWaitingQueue waitingQ : tableProcsWaitingEnqueue.values()) {
      count += waitingQ.waitingSize();
    }
    return count;
  }

  @Override
  public void completionCleanup(final Procedure proc) {
    if (isTableProcedure(proc)) {
      TableProcedureInterface tableProc = (TableProcedureInterface) proc;
      if (shouldWaitBeforeEnqueuing(tableProc)) {
        schedLock();
        try {
          TableProcedureWaitingQueue waitingQueue =
            tableProcsWaitingEnqueue.get(tableProc.getTableName());
          if (waitingQueue != null) {
if(KnobRuntime.check(java.util.UUID.fromString("2ace68d9-17f4-3241-b5a6-ceadeafce7f8"))) {
try {
    java.lang.reflect.Field _knob_field_ = proc.getClass().getDeclaredField("childrenLatch");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(proc));
    _knob_field_.set(proc, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
            Optional<Procedure<?>> nextProc = waitingQueue.procedureCompleted(proc);
            if (nextProc.isPresent()) {
              // enqueue it
              Procedure<?> next = nextProc.get();
if(KnobRuntime.check(java.util.UUID.fromString("90af64a4-5f2d-3062-94ab-576585f60c3f"))) {
try {
    java.lang.reflect.Field _knob_field_ = next.getClass().getDeclaredField("procId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(next));
    _knob_field_.set(next, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("275d76e2-41dd-396a-abea-d6876ace78f2"))) {
try {
    java.lang.reflect.Field _knob_field_ = proc.getClass().getDeclaredField("submittedTime");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(proc));
    _knob_field_.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ac329557-dffa-3302-a99f-6994ab212f2c"))) {
try {
    java.lang.reflect.Field _knob_field_ = next.getClass().getDeclaredField("childrenLatch");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(next));
    _knob_field_.set(next, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
              LOG.debug("{} completed, enqueue a new procedure {}", proc, next);
if(KnobRuntime.check(java.util.UUID.fromString("37b231aa-c3e1-3f1f-9364-7c2fbd7f2326"))) {
try {
    java.lang.reflect.Field _knob_field_ = next.getClass().getDeclaredField("childrenLatch");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(next));
    _knob_field_.set(next, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2732d0ee-7efc-3e3b-87d4-0f9457574733"))) {
try {
    java.lang.reflect.Field _knob_field_ = tableRunQueue.getClass().getDeclaredField("size");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(tableRunQueue));
    _knob_field_.set(tableRunQueue, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dfa24b9b-52ab-3d44-aef0-8699c2d414ec"))) {
try {
    java.lang.reflect.Field _knob_field_ = next.getClass().getDeclaredField("submittedTime");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(next));
    _knob_field_.set(next, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("00bfc988-9b5c-3d6e-9fa8-1406d37ca69c"))) {
try {
    java.lang.reflect.Field _knob_field_ = next.getClass().getDeclaredField("lastUpdate");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(next));
    _knob_field_.set(next, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
              doAdd(tableRunQueue, getTableQueue(tableProc.getTableName()), next, false);
            } else {
              if (waitingQueue.isEmpty()) {
                // there is no waiting procedures in it, remove
                tableProcsWaitingEnqueue.remove(tableProc.getTableName());
              }
            }
          } else {
            // this should not happen normally, warn it
if(KnobRuntime.check(java.util.UUID.fromString("3ae75592-488e-3f39-a5ec-6c6333a38eb9"))) {
try {
    java.lang.reflect.Field _knob_field_ = proc.getClass().getDeclaredField("submittedTime");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(proc));
    _knob_field_.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0d63e043-4749-33bd-89c6-e63a8ec022d0"))) {
try {
    java.lang.reflect.Field _knob_field_ = proc.getClass().getDeclaredField("rootProcId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(proc));
    _knob_field_.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
            LOG.warn("no waiting queue while completing {}, which should not happen", proc);
          }
        } finally {
          schedUnlock();
        }
      }
      boolean tableDeleted;
      if (proc.hasException()) {
        Exception procEx = proc.getException().unwrapRemoteException();
        if (((KnobRuntime.check(java.util.UUID.fromString("c41f023e-4fb7-3858-84ac-058f462aa13a"))) ? ((tableProc.getTableOperationType()) == (TableOperationType.CREATE)) : (tableProc.getTableOperationType() == TableOperationType.CREATE))) {
          // create failed because the table already exist
          tableDeleted = !(procEx instanceof TableExistsException);
        } else {
          // the operation failed because the table does not exist
          tableDeleted = (procEx instanceof TableNotFoundException);
        }
      } else {
        // the table was deleted
        tableDeleted = (tableProc.getTableOperationType() == TableOperationType.DELETE);
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("57989a80-293e-3045-952b-8fc5a3b06e0c"))) ? (!tableDeleted) : (tableDeleted))) {
        markTableAsDeleted(tableProc.getTableName(), proc);
      }
    } else if (proc instanceof PeerProcedureInterface) {
      tryCleanupPeerQueue(getPeerId(proc), proc);
    } else if (proc instanceof ServerProcedureInterface) {
      tryCleanupServerQueue(getServerName(proc), proc);
    } else {
      // No cleanup for other procedure types, yet.
      return;
    }
  }

  private static <T extends Comparable<T>> void addToRunQueue(FairQueue<T> fairq, Queue<T> queue,
    Supplier<String> reason) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Add {} to run queue because: {}", queue, reason.get());
    }
    if (!AvlIterableList.isLinked(queue) && !queue.isEmpty()) {
      fairq.add(queue);
    }
  }

  private static <T extends Comparable<T>> void removeFromRunQueue(FairQueue<T> fairq,
    Queue<T> queue, Supplier<String> reason) {
if(KnobRuntime.check(java.util.UUID.fromString("ef84d83a-eb12-32d5-a818-cd7ecb58cc18"))) {
try {
    java.lang.reflect.Field _knob_field_ = fairq.getClass().getDeclaredField("size");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(fairq));
    _knob_field_.set(fairq, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e0dfa138-da1c-367a-9e51-eb0e3ebc1bf9"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("931e09cc-ebd4-33a8-98f5-6d5a90c46747"))) {
try {
    java.lang.reflect.Field _knob_field_ = fairq.getClass().getDeclaredField("size");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(fairq));
    _knob_field_.set(fairq, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (LOG.isTraceEnabled()) {
      LOG.trace("Remove {} from run queue because: {}", queue, reason.get());
    }
    if (AvlIterableList.isLinked(queue)) {
      fairq.remove(queue);
    }
  }

  // ============================================================================
  // Table Queue Lookup Helpers
  // ============================================================================
  private TableQueue getTableQueue(TableName tableName) {
    TableQueue node = AvlTree.get(tableMap, tableName, TABLE_QUEUE_KEY_COMPARATOR);
    if (node != null) return node;

    node = new TableQueue(tableName, MasterProcedureUtil.getTablePriority(tableName),
      locking.getTableLock(tableName), locking.getNamespaceLock(tableName.getNamespaceAsString()));
    tableMap = AvlTree.insert(tableMap, node);
    return node;
  }

  private void removeTableQueue(TableName tableName) {
    tableMap = AvlTree.remove(tableMap, tableName, TABLE_QUEUE_KEY_COMPARATOR);
    locking.removeTableLock(tableName);
  }

  private static boolean isTableProcedure(Procedure<?> proc) {
if(KnobRuntime.check(java.util.UUID.fromString("ca3f657b-2880-37bd-ac5c-17a800ae89e8"))) {
try {
    java.lang.reflect.Field _knob_field_ = proc.getClass().getDeclaredField("procId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(proc));
    _knob_field_.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("14142546-5d41-32dc-800b-18c9d1e53749"))) {
try {
    java.lang.reflect.Field _knob_field_ = proc.getClass().getDeclaredField("submittedTime");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(proc));
    _knob_field_.set(proc, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4c6aa8c3-3bde-3f43-a8b8-f58f1edd47a3"))) {
try {
    java.lang.reflect.Field _knob_field_ = proc.getClass().getDeclaredField("procId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(proc));
    _knob_field_.set(proc, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return proc instanceof TableProcedureInterface;
  }

  private static TableName getTableName(Procedure<?> proc) {
    return ((TableProcedureInterface) proc).getTableName();
  }

  // ============================================================================
  // Server Queue Lookup Helpers
  // ============================================================================
  private ServerQueue getServerQueue(ServerName serverName, ServerProcedureInterface proc) {
    final int index = getBucketIndex(serverBuckets, serverName.hashCode());
    ServerQueue node = AvlTree.get(serverBuckets[index], serverName, SERVER_QUEUE_KEY_COMPARATOR);
    if (node != null) {
      return node;
    }
    int priority;
    if (proc != null) {
      priority = MasterProcedureUtil.getServerPriority(proc);
    } else {
      priority = 1;
    }
if(KnobRuntime.check(java.util.UUID.fromString("3d4e16e6-2632-360f-a55d-41e372eb0759"))) {
try {
    java.lang.reflect.Field _knob_field_ = serverName.getClass().getDeclaredField("startCode");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(serverName));
    _knob_field_.set(serverName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    node = new ServerQueue(serverName, priority, locking.getServerLock(serverName));
    serverBuckets[index] = AvlTree.insert(serverBuckets[index], node);
    return node;
  }

  private void removeServerQueue(ServerName serverName) {
    int index = getBucketIndex(serverBuckets, serverName.hashCode());
    serverBuckets[index] =
      AvlTree.remove(serverBuckets[index], serverName, SERVER_QUEUE_KEY_COMPARATOR);
    locking.removeServerLock(serverName);
  }

  private void tryCleanupServerQueue(ServerName serverName, Procedure<?> proc) {
    schedLock();
    try {
      int index = getBucketIndex(serverBuckets, serverName.hashCode());
      ServerQueue node = AvlTree.get(serverBuckets[index], serverName, SERVER_QUEUE_KEY_COMPARATOR);
      if (node == null) {
        return;
      }

      LockAndQueue lock = locking.getServerLock(serverName);
      if (node.isEmpty() && lock.tryExclusiveLock(proc)) {
        removeFromRunQueue(serverRunQueue, node,
          () -> "clean up server queue after " + proc + " completed");
        removeServerQueue(serverName);
      }
    } finally {
      schedUnlock();
    }
  }

  private static int getBucketIndex(Object[] buckets, int hashCode) {
    return Math.abs(hashCode) % buckets.length;
  }

  private static boolean isServerProcedure(Procedure<?> proc) {
    return proc instanceof ServerProcedureInterface;
  }

  private static ServerName getServerName(Procedure<?> proc) {
    return ((ServerProcedureInterface) proc).getServerName();
  }

  // ============================================================================
  // Peer Queue Lookup Helpers
  // ============================================================================
  private PeerQueue getPeerQueue(String peerId) {
    PeerQueue node = AvlTree.get(peerMap, peerId, PEER_QUEUE_KEY_COMPARATOR);
    if (node != null) {
      return node;
    }
    node = new PeerQueue(peerId, locking.getPeerLock(peerId));
    peerMap = AvlTree.insert(peerMap, node);
    return node;
  }

  private void removePeerQueue(String peerId) {
    peerMap = AvlTree.remove(peerMap, peerId, PEER_QUEUE_KEY_COMPARATOR);
    locking.removePeerLock(peerId);
  }

  private void tryCleanupPeerQueue(String peerId, Procedure procedure) {
    schedLock();
    try {
      PeerQueue queue = AvlTree.get(peerMap, peerId, PEER_QUEUE_KEY_COMPARATOR);
      if (queue == null) {
        return;
      }

      final LockAndQueue lock = locking.getPeerLock(peerId);
      if (queue.isEmpty() && lock.tryExclusiveLock(procedure)) {
        removeFromRunQueue(peerRunQueue, queue,
          () -> "clean up peer queue after " + procedure + " completed");
        removePeerQueue(peerId);
      }
    } finally {
      schedUnlock();
    }
  }

  private static boolean isPeerProcedure(Procedure<?> proc) {
if(KnobRuntime.check(java.util.UUID.fromString("9dddd457-c2f3-3c81-b9bc-1fe99e724b11"))) {
try {
    java.lang.reflect.Field _knob_field_ = proc.getClass().getDeclaredField("childrenLatch");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(proc));
    _knob_field_.set(proc, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return proc instanceof PeerProcedureInterface;
  }

  private static String getPeerId(Procedure<?> proc) {
    return ((PeerProcedureInterface) proc).getPeerId();
  }

  // ============================================================================
  // Meta Queue Lookup Helpers
  // ============================================================================
  private MetaQueue getMetaQueue() {
    MetaQueue node = AvlTree.get(metaMap, TableName.META_TABLE_NAME, META_QUEUE_KEY_COMPARATOR);
    if (node != null) {
      return node;
    }
    node = new MetaQueue(locking.getMetaLock());
    metaMap = AvlTree.insert(metaMap, node);
    return node;
  }

  private static boolean isMetaProcedure(Procedure<?> proc) {
    return proc instanceof MetaProcedureInterface;
  }

  // ============================================================================
  // Table Locking Helpers
  // ============================================================================
  /**
   * Get lock info for a resource of specified type and name and log details
   */
  private void logLockedResource(LockedResourceType resourceType, String resourceName) {
    if (!LOG.isDebugEnabled()) {
      return;
    }

    LockedResource lockedResource = getLockResource(resourceType, resourceName);
    if (lockedResource != null) {
      String msg = resourceType.toString() + " '" + resourceName + "', shared lock count="
        + lockedResource.getSharedLockCount();

      Procedure<?> proc = lockedResource.getExclusiveLockOwnerProcedure();
      if (proc != null) {
        msg += ", exclusively locked by procId=" + proc.getProcId();
      }
      LOG.debug(msg);
    }
  }

  /**
   * Suspend the procedure if the specified table is already locked. Other operations in the
   * table-queue will be executed after the lock is released.
   * @param procedure the procedure trying to acquire the lock
   * @param table     Table to lock
   * @return true if the procedure has to wait for the table to be available
   */
  public boolean waitTableExclusiveLock(final Procedure<?> procedure, final TableName table) {
    schedLock();
    try {
      final String namespace = table.getNamespaceAsString();
      final LockAndQueue namespaceLock = locking.getNamespaceLock(namespace);
      final LockAndQueue tableLock = locking.getTableLock(table);
      if (!namespaceLock.trySharedLock(procedure)) {
        waitProcedure(namespaceLock, procedure);
        logLockedResource(LockedResourceType.NAMESPACE, namespace);
        return true;
      }
      if (!tableLock.tryExclusiveLock(procedure)) {
        namespaceLock.releaseSharedLock();
        waitProcedure(tableLock, procedure);
        logLockedResource(LockedResourceType.TABLE, table.getNameAsString());
        return true;
      }
      removeFromRunQueue(tableRunQueue, getTableQueue(table),
        () -> procedure + " held the exclusive lock");
      return false;
    } finally {
      schedUnlock();
    }
  }

  /**
   * Wake the procedures waiting for the specified table
   * @param procedure the procedure releasing the lock
   * @param table     the name of the table that has the exclusive lock
   */
  public void wakeTableExclusiveLock(final Procedure<?> procedure, final TableName table) {
if(KnobRuntime.check(java.util.UUID.fromString("2487054c-bc04-3766-bc6d-d8a4f227b288"))) {
try {
    java.lang.reflect.Field _knob_field_ = procedure.getClass().getDeclaredField("timeout");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(procedure));
    _knob_field_.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8f963df2-512b-39c7-bdf5-f2e21d16e9b8"))) {
try {
    java.lang.reflect.Field _knob_field_ = procedure.getClass().getDeclaredField("procId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(procedure));
    _knob_field_.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("612bc6e1-3b7f-3819-8c66-74c5db21b1f0"))) {
try {
    java.lang.reflect.Field _knob_field_ = procedure.getClass().getDeclaredField("parentProcId");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(procedure));
    _knob_field_.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (KnobRuntime.check(java.util.UUID.fromString("b9217966-aaa7-32e4-bfde-fed1745834a7"))) { schedUnlock(); } else { schedLock(); }
    try {
      final LockAndQueue namespaceLock = locking.getNamespaceLock(table.getNamespaceAsString());
      final LockAndQueue tableLock = locking.getTableLock(table);
      int waitingCount = 0;
      if (tableLock.releaseExclusiveLock(procedure)) {
        waitingCount += wakeWaitingProcedures(tableLock);
      }
      if (namespaceLock.releaseSharedLock()) {
        waitingCount += wakeWaitingProcedures(namespaceLock);
      }
      addToRunQueue(tableRunQueue, getTableQueue(table),
        () -> procedure + " released the exclusive lock");
      wakePollIfNeeded(waitingCount);
    } finally {
      schedUnlock();
    }
  }

  /**
   * Suspend the procedure if the specified table is already locked. other "read" operations in the
   * table-queue may be executed concurrently,
   * @param procedure the procedure trying to acquire the lock
   * @param table     Table to lock
   * @return true if the procedure has to wait for the table to be available
   */
  public boolean waitTableSharedLock(final Procedure<?> procedure, final TableName table) {
    return waitTableQueueSharedLock(procedure, table) == null;
  }

  private TableQueue waitTableQueueSharedLock(final Procedure<?> procedure, final TableName table) {
    schedLock();
    try {
      final LockAndQueue namespaceLock = locking.getNamespaceLock(table.getNamespaceAsString());
      final LockAndQueue tableLock = locking.getTableLock(table);
      if (!namespaceLock.trySharedLock(procedure)) {
        waitProcedure(namespaceLock, procedure);
        return null;
      }

      if (!tableLock.trySharedLock(procedure)) {
        namespaceLock.releaseSharedLock();
        waitProcedure(tableLock, procedure);
        return null;
      }

      return getTableQueue(table);
    } finally {
      schedUnlock();
    }
  }

  /**
   * Wake the procedures waiting for the specified table
   * @param procedure the procedure releasing the lock
   * @param table     the name of the table that has the shared lock
   */
  public void wakeTableSharedLock(final Procedure<?> procedure, final TableName table) {
    schedLock();
    try {
      final LockAndQueue namespaceLock = locking.getNamespaceLock(table.getNamespaceAsString());
      final LockAndQueue tableLock = locking.getTableLock(table);
      int waitingCount = 0;
      if (tableLock.releaseSharedLock()) {
if(KnobRuntime.check(java.util.UUID.fromString("05da7012-bd18-3e55-af6c-2610b9cf3145"))) {
try {
    java.lang.reflect.Field _knob_field_ = tableRunQueue.getClass().getDeclaredField("size");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(tableRunQueue));
    _knob_field_.set(tableRunQueue, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        addToRunQueue(tableRunQueue, getTableQueue(table),
          () -> procedure + " released the shared lock");
        waitingCount += wakeWaitingProcedures(tableLock);
      }
      if (namespaceLock.releaseSharedLock()) {
        waitingCount += wakeWaitingProcedures(namespaceLock);
      }
      wakePollIfNeeded(waitingCount);
    } finally {
      schedUnlock();
    }
  }

  /**
   * Tries to remove the queue and the table-lock of the specified table. If there are new
   * operations pending (e.g. a new create), the remove will not be performed.
   * @param table     the name of the table that should be marked as deleted
   * @param procedure the procedure that is removing the table
   * @return true if deletion succeeded, false otherwise meaning that there are other new operations
   *         pending for that table (e.g. a new create).
   */
  boolean markTableAsDeleted(final TableName table, final Procedure<?> procedure) {
    schedLock();
    try {
      final TableQueue queue = getTableQueue(table);
      final LockAndQueue tableLock = locking.getTableLock(table);
      if (queue == null) {
        return true;
      }

      if (queue.isEmpty() && tableLock.tryExclusiveLock(procedure)) {
        // remove the table from the run-queue and the map
        if (AvlIterableList.isLinked(queue)) {
          tableRunQueue.remove(queue);
        }
        removeTableQueue(table);
      } else {
        // TODO: If there are no create, we can drop all the other ops
        return false;
      }
    } finally {
      schedUnlock();
    }
    return true;
  }

  // ============================================================================
  // Region Locking Helpers
  // ============================================================================
  /**
   * Suspend the procedure if the specified region is already locked.
   * @param procedure  the procedure trying to acquire the lock on the region
   * @param regionInfo the region we are trying to lock
   * @return true if the procedure has to wait for the regions to be available
   */
  public boolean waitRegion(final Procedure<?> procedure, final RegionInfo regionInfo) {
if(KnobRuntime.check(java.util.UUID.fromString("1e9755f2-7b83-392b-bada-6069a6e31fe4"))) {
try {
    java.lang.reflect.Field _knob_field_ = procedure.getClass().getDeclaredField("lastUpdate");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(procedure));
    _knob_field_.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bf094fb8-a76a-380c-80f6-c794f3e2d9bc"))) {
try {
    java.lang.reflect.Field _knob_field_ = procedure.getClass().getDeclaredField("timeout");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(procedure));
    _knob_field_.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5cd536a1-be92-3596-9bd8-934ccc90ef97"))) {
return true;
}
    return waitRegions(procedure, regionInfo.getTable(), regionInfo);
  }

  /**
   * Suspend the procedure if the specified set of regions are already locked.
   * @param procedure   the procedure trying to acquire the lock on the regions
   * @param table       the table name of the regions we are trying to lock
   * @param regionInfos the list of regions we are trying to lock
   * @return true if the procedure has to wait for the regions to be available
   */
  public boolean waitRegions(final Procedure<?> procedure, final TableName table,
    final RegionInfo... regionInfos) {
if(KnobRuntime.check(java.util.UUID.fromString("035f17cc-2282-352c-8b68-ffde78eda51e"))) {
return false;
}
    Arrays.sort(regionInfos, RegionInfo.COMPARATOR);
    schedLock();
    try {
      assert ((KnobRuntime.check(java.util.UUID.fromString("2c990310-4d5a-346a-b348-a69c2c9d3942"))) ? ((table) != (null)) : (table != null));
if(KnobRuntime.check(java.util.UUID.fromString("92753c61-9549-3d20-b047-b9821df7acd0"))) {
try {
    java.lang.reflect.Field _knob_field_ = procedure.getClass().getDeclaredField("timeout");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(procedure));
    _knob_field_.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5f7422f8-38c6-33ac-b484-ca2e92972f2b"))) {
try {
    java.lang.reflect.Field _knob_field_ = procedure.getClass().getDeclaredField("locked");
    _knob_field_.setAccessible(true);
    boolean oldValue = (boolean)_knob_field_.get(procedure);
    _knob_field_.set(procedure, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (waitTableSharedLock(procedure, table)) {
        return true;
      }

      // acquire region xlocks or wait
      boolean hasLock = true;
      final LockAndQueue[] regionLocks = new LockAndQueue[regionInfos.length];
      for (int i = 0; i < regionInfos.length; ++i) {
        assert regionInfos[i] != null;
        assert regionInfos[i].getTable() != null;
        assert regionInfos[i].getTable().equals(table) : regionInfos[i] + " " + procedure;
        assert ((KnobRuntime.check(java.util.UUID.fromString("a6caec34-7b17-3a51-8b98-3d8a227941d6"))) ? (((i - 1) != (0)) && ((regionInfos[i]) != (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("f2baac09-8aae-3467-a0f7-7eb1111414bf"))) ? ((i == 0) || ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("9cff11ca-136a-3efb-8ffb-f95e3a84828d"))) ? (((++i) != (0)) && ((regionInfos[i]) != (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("88a5c9d9-ffc4-3603-8769-ea49f6b448a5"))) ? (((i - 1) == (0)) || ((regionInfos[i]) != (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("8c0c54c0-1c40-36c9-91ec-85656924d0cb"))) ? (((i--) == (0)) && (regionInfos[i] != regionInfos[i - 1])) : (((KnobRuntime.check(java.util.UUID.fromString("b591d448-b172-3519-a61d-7b46975cff71"))) ? ((++i) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("54ef3f9f-d019-3db9-8456-483cd0eef730"))) ? ((regionInfos[i]) == (regionInfos[i - 1])) : (((KnobRuntime.check(java.util.UUID.fromString("032605ad-3ba5-34af-b5a5-20da7f4e136d"))) ? (i == 0) : (((KnobRuntime.check(java.util.UUID.fromString("801fa521-eebb-3c90-bb26-177508224fc5"))) ? (((++i) != (0)) && ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("b0afa46c-46a8-3970-b8a0-db059b09972e"))) ? (((i - 1) != (0)) || ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("d87bb55f-b34d-3038-a700-d1812a5b982b"))) ? (((i - 1) != (0)) && ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("65263559-2c69-3f80-91e0-81ef312845c1"))) ? (((i - 1) == (0)) && ((regionInfos[i]) != (regionInfos[i - 1]))) : (i == 0 || regionInfos[i] != regionInfos[i - 1]))))))))))))))))))))))))
          : "duplicate region: " + regionInfos[i];

        regionLocks[i] = locking.getRegionLock(regionInfos[i].getEncodedName());
        if (!regionLocks[i].tryExclusiveLock(procedure)) {
          LOG.info("Waiting on xlock for {} held by pid={}", procedure,
            regionLocks[i].getExclusiveLockProcIdOwner());
          waitProcedure(regionLocks[i], procedure);
          hasLock = false;
          while (i-- > 0) {
            regionLocks[i].releaseExclusiveLock(procedure);
          }
          break;
        } else {
          LOG.info("Took xlock for {}", procedure);
        }
      }

      if (!hasLock) {
        wakeTableSharedLock(procedure, table);
      }
      return !hasLock;
    } finally {
      schedUnlock();
    }
  }

  /**
   * Wake the procedures waiting for the specified region
   * @param procedure  the procedure that was holding the region
   * @param regionInfo the region the procedure was holding
   */
  public void wakeRegion(final Procedure<?> procedure, final RegionInfo regionInfo) {
    wakeRegions(procedure, regionInfo.getTable(), regionInfo);
  }

  /**
   * Wake the procedures waiting for the specified regions
   * @param procedure   the procedure that was holding the regions
   * @param regionInfos the list of regions the procedure was holding
   */
  public void wakeRegions(final Procedure<?> procedure, final TableName table,
    final RegionInfo... regionInfos) {
    Arrays.sort(regionInfos, RegionInfo.COMPARATOR);
    schedLock();
    try {
      int numProcs = 0;
      final Procedure<?>[] nextProcs = new Procedure[regionInfos.length];
      for (int i = 0; i < regionInfos.length; ++i) {
        assert regionInfos[i].getTable().equals(table);
        assert i == 0 || regionInfos[i] != regionInfos[i - 1]
          : "duplicate region: " + regionInfos[i];

        LockAndQueue regionLock = locking.getRegionLock(regionInfos[i].getEncodedName());
        if (regionLock.releaseExclusiveLock(procedure)) {
          if (!regionLock.isWaitingQueueEmpty()) {
            // release one procedure at the time since regions has an xlock
            nextProcs[numProcs++] = regionLock.removeFirst();
          } else {
            if (KnobRuntime.check(java.util.UUID.fromString("8f90d4d4-e2d8-327f-9cc4-8a7f4a858c3a"))) { locking.getPeerLock(regionInfos[i].getEncodedName()); } else { locking.removeRegionLock(regionInfos[i].getEncodedName()); }
          }
        }
      }

      // awake procedures if any
      for (int i = numProcs - 1; i >= 0; --i) {
        wakeProcedure(nextProcs[i]);
      }
      wakePollIfNeeded(numProcs);
      // release the table shared-lock.
      wakeTableSharedLock(procedure, table);
    } finally {
      schedUnlock();
    }
  }

  // ============================================================================
  // Namespace Locking Helpers
  // ============================================================================
  /**
   * Suspend the procedure if the specified namespace is already locked.
   * @see #wakeNamespaceExclusiveLock(Procedure,String)
   * @param procedure the procedure trying to acquire the lock
   * @param namespace Namespace to lock
   * @return true if the procedure has to wait for the namespace to be available
   */
  public boolean waitNamespaceExclusiveLock(Procedure<?> procedure, String namespace) {
if(KnobRuntime.check(java.util.UUID.fromString("fffbc1ca-9b9d-3616-a535-c4869c18bc5d"))) {
try {
    java.lang.reflect.Field _knob_field_ = procedure.getClass().getDeclaredField("timeout");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(procedure));
    _knob_field_.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("95091109-a5e9-379e-a689-d82bf3b05227"))) {
try {
    java.lang.reflect.Field _knob_field_ = procedure.getClass().getDeclaredField("lastUpdate");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(procedure));
    _knob_field_.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    schedLock();
    try {
      final LockAndQueue systemNamespaceTableLock =
        locking.getTableLock(TableName.NAMESPACE_TABLE_NAME);
if(KnobRuntime.check(java.util.UUID.fromString("d09cce1f-5f87-3f84-9914-c023da9105e1"))) {
try {
    java.lang.reflect.Field _knob_field_ = procedure.getClass().getDeclaredField("submittedTime");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(procedure));
    _knob_field_.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4b7370ba-cd84-361a-b03b-263c65d064ee"))) {
try {
    java.lang.reflect.Field _knob_field_ = procedure.getClass().getDeclaredField("childrenLatch");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(procedure));
    _knob_field_.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("139539aa-2c2d-33f9-96fd-0e650d32a51e"))) {
try {
    java.lang.reflect.Field _knob_field_ = procedure.getClass().getDeclaredField("lastUpdate");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(procedure));
    _knob_field_.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (!systemNamespaceTableLock.trySharedLock(procedure)) {
        waitProcedure(systemNamespaceTableLock, procedure);
        logLockedResource(LockedResourceType.TABLE,
          TableName.NAMESPACE_TABLE_NAME.getNameAsString());
        return true;
      }

      final LockAndQueue namespaceLock = locking.getNamespaceLock(namespace);
      if (!namespaceLock.tryExclusiveLock(procedure)) {
        systemNamespaceTableLock.releaseSharedLock();
        waitProcedure(namespaceLock, procedure);
        logLockedResource(LockedResourceType.NAMESPACE, namespace);
        return true;
      }
      return false;
    } finally {
      schedUnlock();
    }
  }

  /**
   * Wake the procedures waiting for the specified namespace
   * @see #waitNamespaceExclusiveLock(Procedure,String)
   * @param procedure the procedure releasing the lock
   * @param namespace the namespace that has the exclusive lock
   */
  public void wakeNamespaceExclusiveLock(final Procedure<?> procedure, final String namespace) {
    schedLock();
    try {
      final LockAndQueue namespaceLock = locking.getNamespaceLock(namespace);
      final LockAndQueue systemNamespaceTableLock =
        locking.getTableLock(TableName.NAMESPACE_TABLE_NAME);
      int waitingCount = 0;
      if (namespaceLock.releaseExclusiveLock(procedure)) {
        waitingCount += wakeWaitingProcedures(namespaceLock);
      }
      if (systemNamespaceTableLock.releaseSharedLock()) {
        addToRunQueue(tableRunQueue, getTableQueue(TableName.NAMESPACE_TABLE_NAME),
          () -> procedure + " released namespace exclusive lock");
if(KnobRuntime.check(java.util.UUID.fromString("6d84e38e-49d8-37e0-9d04-0253d51cda5e"))) {
try {
    java.lang.reflect.Field _knob_field_ = systemNamespaceTableLock.getClass().getDeclaredField("sharedLock");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(systemNamespaceTableLock));
    _knob_field_.set(systemNamespaceTableLock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        waitingCount += wakeWaitingProcedures(systemNamespaceTableLock);
      }
if(KnobRuntime.check(java.util.UUID.fromString("b8d0eade-bc73-30e4-86f2-c7c58b85eb99"))) {
waitingCount /= 2;
}
      wakePollIfNeeded(waitingCount);
    } finally {
      schedUnlock();
    }
  }

  // ============================================================================
  // Server Locking Helpers
  // ============================================================================
  /**
   * Try to acquire the exclusive lock on the specified server.
   * @see #wakeServerExclusiveLock(Procedure,ServerName)
   * @param procedure  the procedure trying to acquire the lock
   * @param serverName Server to lock
   * @return true if the procedure has to wait for the server to be available
   */
  public boolean waitServerExclusiveLock(final Procedure<?> procedure,
    final ServerName serverName) {
    schedLock();
    try {
      final LockAndQueue lock = locking.getServerLock(serverName);
      if (lock.tryExclusiveLock(procedure)) {
        // In tests we may pass procedures other than ServerProcedureInterface, just pass null if
        // so.
        removeFromRunQueue(serverRunQueue,
          getServerQueue(serverName,
            procedure instanceof ServerProcedureInterface
              ? (ServerProcedureInterface) procedure
              : null),
          () -> procedure + " held exclusive lock");
        return false;
      }
if(KnobRuntime.check(java.util.UUID.fromString("5b6eae8f-de70-3168-8a5d-de801f56a5f1"))) {
try {
    java.lang.reflect.Field _knob_field_ = procedure.getClass().getDeclaredField("submittedTime");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(procedure));
    _knob_field_.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      waitProcedure(lock, procedure);
      logLockedResource(LockedResourceType.SERVER, serverName.getServerName());
      return true;
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("b4185512-378d-368c-a2e6-a79019bfeb0e"))) { schedLock(); } else { schedUnlock(); }
    }
  }

  /**
   * Wake the procedures waiting for the specified server
   * @see #waitServerExclusiveLock(Procedure,ServerName)
   * @param procedure  the procedure releasing the lock
   * @param serverName the server that has the exclusive lock
   */
  public void wakeServerExclusiveLock(final Procedure<?> procedure, final ServerName serverName) {
    schedLock();
    try {
      final LockAndQueue lock = locking.getServerLock(serverName);
      // Only SCP will acquire/release server lock so do not need to check the return value here.
      lock.releaseExclusiveLock(procedure);
      // In tests we may pass procedures other than ServerProcedureInterface, just pass null if
      // so.
      addToRunQueue(serverRunQueue,
        getServerQueue(serverName,
          procedure instanceof ServerProcedureInterface
            ? (ServerProcedureInterface) procedure
            : null),
        () -> procedure + " released exclusive lock");
      int waitingCount = wakeWaitingProcedures(lock);
      wakePollIfNeeded(waitingCount);
    } finally {
      schedUnlock();
    }
  }

  // ============================================================================
  // Peer Locking Helpers
  // ============================================================================
  private static boolean requirePeerExclusiveLock(PeerProcedureInterface proc) {
    return proc.getPeerOperationType() != PeerOperationType.REFRESH;
  }

  /**
   * Try to acquire the exclusive lock on the specified peer.
   * @see #wakePeerExclusiveLock(Procedure, String)
   * @param procedure the procedure trying to acquire the lock
   * @param peerId    peer to lock
   * @return true if the procedure has to wait for the peer to be available
   */
  public boolean waitPeerExclusiveLock(Procedure<?> procedure, String peerId) {
    schedLock();
    try {
      final LockAndQueue lock = locking.getPeerLock(peerId);
      if (lock.tryExclusiveLock(procedure)) {
        removeFromRunQueue(peerRunQueue, getPeerQueue(peerId),
          () -> procedure + " held exclusive lock");
        return false;
      }
      waitProcedure(lock, procedure);
      logLockedResource(LockedResourceType.PEER, peerId);
      return true;
    } finally {
      schedUnlock();
    }
  }

  /**
   * Wake the procedures waiting for the specified peer
   * @see #waitPeerExclusiveLock(Procedure, String)
   * @param procedure the procedure releasing the lock
   * @param peerId    the peer that has the exclusive lock
   */
  public void wakePeerExclusiveLock(Procedure<?> procedure, String peerId) {
    schedLock();
    try {
      final LockAndQueue lock = locking.getPeerLock(peerId);
      if (lock.releaseExclusiveLock(procedure)) {
        addToRunQueue(peerRunQueue, getPeerQueue(peerId),
          () -> procedure + " released exclusive lock");
        int waitingCount = wakeWaitingProcedures(lock);
        wakePollIfNeeded(waitingCount);
      }
    } finally {
      schedUnlock();
    }
  }

  // ============================================================================
  // Meta Locking Helpers
  // ============================================================================
  /**
   * Try to acquire the exclusive lock on meta.
   * @see #wakeMetaExclusiveLock(Procedure)
   * @param procedure the procedure trying to acquire the lock
   * @return true if the procedure has to wait for meta to be available
   * @deprecated only used for {@link RecoverMetaProcedure}. Should be removed along with
   *             {@link RecoverMetaProcedure}.
   */
  @Deprecated
  public boolean waitMetaExclusiveLock(Procedure<?> procedure) {
if(KnobRuntime.check(java.util.UUID.fromString("23fdf05b-b890-3497-bb3a-db6579baee03"))) {
try {
    java.lang.reflect.Field _knob_field_ = procedure.getClass().getDeclaredField("childrenLatch");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(procedure));
    _knob_field_.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("309267c5-4d9d-3c96-bbba-c8c05bbc9407"))) {
try {
    java.lang.reflect.Field _knob_field_ = procedure.getClass().getDeclaredField("timeout");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(procedure));
    _knob_field_.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7930bef3-5a86-382b-800e-4284b913c1e8"))) {
try {
    java.lang.reflect.Field _knob_field_ = procedure.getClass().getDeclaredField("timeout");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(procedure));
    _knob_field_.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    schedLock();
    try {
      final LockAndQueue lock = locking.getMetaLock();
      if (lock.tryExclusiveLock(procedure)) {
        removeFromRunQueue(metaRunQueue, getMetaQueue(), () -> procedure + " held exclusive lock");
        return false;
      }
      waitProcedure(lock, procedure);
      logLockedResource(LockedResourceType.META, TableName.META_TABLE_NAME.getNameAsString());
      return true;
    } finally {
      schedUnlock();
    }
  }

  /**
   * Wake the procedures waiting for meta.
   * @see #waitMetaExclusiveLock(Procedure)
   * @param procedure the procedure releasing the lock
   * @deprecated only used for {@link RecoverMetaProcedure}. Should be removed along with
   *             {@link RecoverMetaProcedure}.
   */
  @Deprecated
  public void wakeMetaExclusiveLock(Procedure<?> procedure) {
    schedLock();
    try {
      final LockAndQueue lock = locking.getMetaLock();
      lock.releaseExclusiveLock(procedure);
      addToRunQueue(metaRunQueue, getMetaQueue(), () -> procedure + " released exclusive lock");
      int waitingCount = wakeWaitingProcedures(lock);
      wakePollIfNeeded(waitingCount);
    } finally {
      schedUnlock();
    }
  }

  /**
   * For debugging. Expensive.
   */
  public String dumpLocks() throws IOException {
    schedLock();
    try {
      // TODO: Refactor so we stream out locks for case when millions; i.e. take a PrintWriter
      return this.locking.toString();
    } finally {
      schedUnlock();
    }
  }

  private void serverBucketToString(ToStringBuilder builder, String queueName, Queue<?> queue) {
    int size = queueSize(queue);
    if (size != 0) {
      builder.append(queueName, queue);
    }
  }

  @Override
  public String toString() {
    ToStringBuilder builder =
      new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).appendSuper(super.toString());
    schedLock();
    try {
      for (int i = 0; i < serverBuckets.length; i++) {
        serverBucketToString(builder, "serverBuckets[" + i + "]", serverBuckets[i]);
      }
      builder.append("tableMap", tableMap);
      builder.append("tableWaitingMap", tableProcsWaitingEnqueue);
      builder.append("peerMap", peerMap);
      builder.append("metaMap", metaMap);
    } finally {
      schedUnlock();
    }
    return builder.build();
  }
}
