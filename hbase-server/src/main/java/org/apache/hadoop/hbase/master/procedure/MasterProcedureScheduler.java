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
if(KnobRuntime.check(java.util.UUID.fromString("13c05f64-50c5-3b5a-b393-f5297517abf1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7cb5af92-fe9f-33b3-9941-5666f09e40b2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5957b3c2-d99a-3569-82c8-a4feb9bce004"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b0e3b7dd-5cc1-3cf1-a58f-79f2d027809b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8bd3b947-6c2a-30ac-8ac6-1e7d46771d56"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("218889f9-d7f5-3a4e-82f7-e276389d5452"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0d99b814-a596-39f9-a84f-c6e9aa1a2146"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("f540e5cc-e56d-342b-b919-37362c6115da"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("de67f03f-de7f-3ce6-b89a-0eb085b77719"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8aaa5579-4321-3f7e-9dfb-546aca714261"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fb6d9aef-5a57-36d6-8c67-153d49616064"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0ddd952f-3c11-3d54-b4d1-29333518833e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9ac9b97e-b5da-371a-8522-b4144b445d7c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("190da017-2385-319e-bc06-5ef4a40d41d6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3e39d9dc-9c43-3f26-979b-056247dd54d1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d40d58bc-ca59-35e2-a89d-c4d9a7c837b9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("08f4e774-d8c8-3a28-afd3-5a9415e54877"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a4412be3-261b-3411-9873-f7bac550bf55"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("27130dd8-5bb4-3455-b118-14dd121a6e04"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("eed09352-fc63-3fb8-9ea7-a48b34e5145e"))) {
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
    push(proc, false, true);
  }

  private boolean shouldWaitBeforeEnqueuing(TableProcedureInterface proc) {
    return TableQueue.requireTableExclusiveLock(proc);
  }

  @Override
  protected void enqueue(final Procedure proc, final boolean addFront) {
    if (isMetaProcedure(proc)) {
if(KnobRuntime.check(java.util.UUID.fromString("5034d77c-6f64-341d-a985-55614c375ae0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6c190c51-7641-393e-98dc-014eb2fb952c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c42258fc-4772-3df3-b4be-70b0adfa033a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d5897475-eb16-333b-a905-8be4572140cf"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9c5a3b52-6a7b-3a8a-b56d-3acaf1fd2c26"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("68c950f0-5298-3084-ba9b-a325a86e3547"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("33211eb8-93cc-33c3-8d42-37e7bb88b3b2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fca53dfa-dfaf-3740-a494-66839e4b0057"))) {
try {
    java.lang.reflect.Field field = metaRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaRunQueue));
    field.set(metaRunQueue, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8702b7a5-099d-3701-84da-9ee9094e737e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cd912b8d-1090-3e91-a9da-9d303ac21374"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9aa03e33-896c-3dcf-bed3-f0f1809202da"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b5dfbdfd-e8f6-3eac-9936-7aa9356172d4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8660f2d6-19bd-36e7-81c7-964f10408bcb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b53009db-fbb3-358b-b907-5c35076ca04f"))) {
try {
    java.lang.reflect.Field field = metaRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaRunQueue));
    field.set(metaRunQueue, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0840212d-4796-3b46-9ba0-bf534bab13af"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cd09c969-2d02-38de-9610-7590ce08a64c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7df5d7be-42a5-3cda-8c36-fc0e059fe086"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("05744ff7-b65b-3f98-b47a-64b5ef003a18"))) {
try {
    java.lang.reflect.Field field = metaRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaRunQueue));
    field.set(metaRunQueue, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1c297c6a-f630-3fff-915d-0255f1a55554"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7ffee3f8-bde1-3e2e-b134-a76ad8c78ec3"))) {
try {
    java.lang.reflect.Field field = metaRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(metaRunQueue));
    field.set(metaRunQueue, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fd23937d-6bf5-3dd9-99eb-d7572e27acd9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2cb20a1b-9fbb-3c51-9cd6-7a461c3cc0fb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3f74d324-c225-35f7-81d1-042ac1ef74b7"))) {
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
      if (KnobRuntime.check(java.util.UUID.fromString("c1360e1f-5a5f-34a7-9f8f-d068200ea04e"))) { doAdd(metaRunQueue, getMetaQueue(), proc, !addFront); } else { doAdd(metaRunQueue, getMetaQueue(), proc, addFront); }
    } else if (isTableProcedure(proc)) {
      TableProcedureInterface tableProc = (TableProcedureInterface) proc;
      if (shouldWaitBeforeEnqueuing(tableProc)) {
        TableProcedureWaitingQueue waitingQueue = tableProcsWaitingEnqueue.computeIfAbsent(
          tableProc.getTableName(), k -> new TableProcedureWaitingQueue(procedureRetriever));
        if (!waitingQueue.procedureSubmitted(proc)) {
          // there is a table procedure for this table already enqueued, waiting
if(KnobRuntime.check(java.util.UUID.fromString("6dd817a3-439c-3bc7-9186-7f68124ce0fc"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a80c88e1-90d5-33a9-9c31-58894b35ea80"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5b87bd11-6b71-3b5d-9c49-e330dc3f6594"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("47a78f43-763b-38fe-b6c6-b51e124225ac"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8509bd96-d269-382b-a99c-0a351a9a7a26"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f2975f61-6137-3783-8846-9bf857bd2374"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e61dfa31-c9b3-3705-acd4-25e3759aff5b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4be61f7f-30e6-361e-9b1d-6ec0e30d543a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("172eac9c-517a-3a94-aad6-6ab7ebd413b2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ec03faa8-d712-3d59-be82-f25ff7f731aa"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b716e3fd-5dd8-3f81-8006-4f0ba207e486"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("daa1be1f-8ac1-3803-8bf2-e284eeaaadb6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f2c46f75-2b05-3904-9669-16e860655c65"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fee609ec-cdb2-3441-b14a-7bba12f2eeae"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fa4bb84b-60b8-35e6-be2e-269443f7cb64"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("71a52136-5267-3480-9ce4-87d97660020c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b52a3eff-efb7-336b-97af-2596bd5e9d38"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a3eccf9c-44f6-3028-9e8f-80976359ed17"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d813e964-9fc5-34d8-acfc-62bc8ea01918"))) {
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
    if (((KnobRuntime.check(java.util.UUID.fromString("7052f8a2-d980-3177-974b-2a040d30f442"))) ? ((reason) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d4eb7634-a78d-3163-bb5d-c37bc94e2eec"))) ? ((reason) == (null)) : (reason != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("8406e1cc-e997-3c28-acd5-b05a5d238c7d"))) {
try {
    java.lang.reflect.Field field = fairq.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fairq));
    field.set(fairq, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6152569d-8e75-316b-8810-1e733cdf9c3c"))) {
try {
    java.lang.reflect.Field field = fairq.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fairq));
    field.set(fairq, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4f8797e7-3f2d-31bf-84e6-4725f23077e7"))) {
try {
    java.lang.reflect.Field field = fairq.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fairq));
    field.set(fairq, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5719c93d-2e00-34cd-8a57-25d753221a84"))) {
try {
    java.lang.reflect.Field field = queue.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queue));
    field.set(queue, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f5a73764-24fd-3de4-b898-fb1ece1305fb"))) {
try {
    java.lang.reflect.Field field = queue.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queue));
    field.set(queue, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cd82af30-5fa5-3649-9daf-aa6d971d2db7"))) {
try {
    java.lang.reflect.Field field = fairq.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fairq));
    field.set(fairq, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("25d0a034-8697-3323-86a5-a7b2122378ad"))) {
try {
    java.lang.reflect.Field field = queue.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queue));
    field.set(queue, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fd70ac22-8a69-37fa-a021-272647634bee"))) {
try {
    java.lang.reflect.Field field = queue.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queue));
    field.set(queue, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (KnobRuntime.check(java.util.UUID.fromString("029680fe-04ee-3a59-97e6-6f050f1900cd"))) { removeFromRunQueue(fairq, queue, reason); } else { addToRunQueue(fairq, queue, reason); }
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
if(KnobRuntime.check(java.util.UUID.fromString("6032aa09-c406-302e-8434-bca70983fe8e"))) {
try {
    java.lang.reflect.Field field = tableRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableRunQueue));
    field.set(tableRunQueue, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("687aebef-af0b-3a26-a507-410a74a69712"))) {
try {
    java.lang.reflect.Field field = tableRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableRunQueue));
    field.set(tableRunQueue, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cad63169-6f00-3bdc-ba74-02fd6b0bb051"))) {
try {
    java.lang.reflect.Field field = tableRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableRunQueue));
    field.set(tableRunQueue, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2cfb6c2f-a78b-3f9b-a79b-a44b428a7624"))) {
try {
    java.lang.reflect.Field field = tableRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableRunQueue));
    field.set(tableRunQueue, oldValue * 2);
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
if(KnobRuntime.check(java.util.UUID.fromString("a0db884f-b600-3759-b77d-320a5717a8db"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aa0a7a8c-0e29-3880-92f6-84fe65de9a55"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cedd96bb-a474-3129-a0ea-105e9d36762e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ca64905e-3ddb-3d43-a892-cb9c8b07e128"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a3ee02fc-f5b2-3d25-8ee0-0fda4c325ad1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a7992a84-97bd-36a0-9d4c-a1458e84b265"))) {
try {
    java.lang.reflect.Field field = rq.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(rq));
    field.set(rq, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6abf432a-fbf7-3298-8353-2a7be4f89f4e"))) {
try {
    java.lang.reflect.Field field = rq.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(rq));
    field.set(rq, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8c2f0010-9a24-30f0-8513-bdbab7add422"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9b4025d5-d995-38fa-aba0-0a31c15dc370"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f09f414e-2812-3e14-90e2-605e97289e03"))) {
try {
    java.lang.reflect.Field field = rq.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(rq));
    field.set(rq, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dca451ea-b073-31ca-bc1d-b09fe28570e2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f2922909-b192-3f53-aa1c-67d0e6502dfb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("273ecc1b-c6ce-3ee7-aa86-da53618d76af"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0c7c7531-1987-3210-b294-f10814270e8b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("709845e9-9ca9-3138-915b-e108dc0332a9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ae310302-a9b3-3165-a0df-1fc1857fb7fe"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2cd9a956-3990-3050-ac4d-1e0b6edab149"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("5d214b3d-0abb-3862-a4eb-f692d74e3539"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4b599817-d71e-3c3f-8caa-b6bbc6bc75d4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0eb9c0a3-f4a8-341f-b72e-a14e1d87b4bd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5d2d0c88-d8d0-38be-9b7b-6914e1163084"))) {
try {
    java.lang.reflect.Field field = rq.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(rq));
    field.set(rq, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2a5bd611-86df-32f0-af01-f6c0e8b946da"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6320ad55-303a-36b4-ac12-7dd1f24d79f5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9da9d905-56bc-3795-a06a-9022f21e0f7a"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("dc6909f1-631e-31ac-b69b-fb77504cf057"))) {
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
    if (((KnobRuntime.check(java.util.UUID.fromString("628552d4-ca5b-32b2-8424-b06affa3fc50"))) ? ((rq) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("154f7f75-fdf9-307f-a8b5-1d9f37c30ffc"))) ? (((rq) == (null)) && (!rq.isAvailable())) : (((KnobRuntime.check(java.util.UUID.fromString("d50cdd97-3c59-3595-91d6-f4ba6e16f03d"))) ? (((rq) == (null)) || (!rq.isAvailable())) : (((KnobRuntime.check(java.util.UUID.fromString("3a2085d9-100c-3f1d-89d1-e8b467230ef5"))) ? (((rq) != (null)) || (!rq.isAvailable())) : (((KnobRuntime.check(java.util.UUID.fromString("d825c32f-dbf6-3ce3-a69b-34f3c10ff292"))) ? (rq == null) : (((KnobRuntime.check(java.util.UUID.fromString("6bd3eb84-a41c-3228-ae4a-e6a00bba777a"))) ? (((rq) != (null)) && (!rq.isAvailable())) : (((KnobRuntime.check(java.util.UUID.fromString("3b82dd48-4c16-3ae0-9ed3-0818024e7daa"))) ? (!rq.isAvailable()) : (((KnobRuntime.check(java.util.UUID.fromString("14967876-25cf-3e30-a6ce-b91f4d1a6009"))) ? ((rq == null) && (!rq.isAvailable())) : (((KnobRuntime.check(java.util.UUID.fromString("742baafe-676c-3a41-b32c-d93304e9dd1b"))) ? ((rq) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("67eee030-8581-383f-87b3-a850e55954b8"))) ? ((rq == null) || (!rq.isAvailable())) : (rq == null || !rq.isAvailable()))))))))))))))))))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2f21974f-7ec1-3b3f-93d3-4558db8254f2"))) {
try {
    java.lang.reflect.Field field = fairq.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fairq));
    field.set(fairq, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aad1167d-ab68-3f52-b52f-d4f4ffca5cfe"))) {
try {
    java.lang.reflect.Field field = fairq.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fairq));
    field.set(fairq, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4704cffa-9734-368e-bc41-36b4f4963ff1"))) {
try {
    java.lang.reflect.Field field = rq.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(rq));
    field.set(rq, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("914a9c92-41be-385a-9fe3-4987b3f4365e"))) {
try {
    java.lang.reflect.Field field = rq.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(rq));
    field.set(rq, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a3274d5d-3bc7-3cae-a8d4-8a34e494aa0a"))) {
try {
    java.lang.reflect.Field field = rq.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(rq));
    field.set(rq, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fba90488-8529-3988-9980-1e323ce4baa0"))) {
try {
    java.lang.reflect.Field field = fairq.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fairq));
    field.set(fairq, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("43ac4401-17ee-3b66-a71d-0242cbca4e80"))) {
try {
    java.lang.reflect.Field field = fairq.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fairq));
    field.set(fairq, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5773adaf-cfe0-3bdd-a71b-7427b5a1ebf2"))) {
try {
    java.lang.reflect.Field field = rq.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(rq));
    field.set(rq, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (KnobRuntime.check(java.util.UUID.fromString("8668fa90-4b96-3b86-9f58-c4ae371d3fbf"))) { addToRunQueue(fairq, rq, () -> "no procedure can be executed"); } else { removeFromRunQueue(fairq, rq, () -> "no procedure can be executed"); }
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

    assert ((KnobRuntime.check(java.util.UUID.fromString("1bec6b77-2a89-3770-a9b5-2cb8f2b79f41"))) ? ((size()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8e1dde76-0eaf-3de4-adfa-babc5a74947e"))) ? ((size()) != (0)) : (size() == 0)))) : "expected queue size to be 0, got " + size();
  }

  private <T extends Comparable<T>, TNode extends Queue<T>> void clear(TNode treeMap,
    FairQueue<T> fairq, AvlKeyComparator<TNode> comparator) {
if(KnobRuntime.check(java.util.UUID.fromString("220ccccc-0c2c-3c59-ad7a-5f8bcef20db5"))) {
try {
    java.lang.reflect.Field field = fairq.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fairq));
    field.set(fairq, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c22ff260-a9fc-3207-bdce-6bd831aafef3"))) {
try {
    java.lang.reflect.Field field = fairq.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fairq));
    field.set(fairq, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bed3336b-3f6d-3c9d-b080-48f1612da751"))) {
try {
    java.lang.reflect.Field field = fairq.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fairq));
    field.set(fairq, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("889bf759-a23a-3a71-8117-970f9a6853ca"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("a014338f-d804-33db-a509-6813f3585eaa"))) {
try {
    java.lang.reflect.Field field = fairq.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fairq));
    field.set(fairq, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    while (((KnobRuntime.check(java.util.UUID.fromString("a2a7cf5f-c036-3253-b290-8b50bec60130"))) ? ((treeMap) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("84630801-b806-34f2-a809-b59d2f2fc250"))) ? ((treeMap) != (null)) : (treeMap != null))))) {
      Queue<T> node = AvlTree.getFirst(treeMap);
      treeMap = AvlTree.remove(treeMap, node.getKey(), comparator);
      if (fairq != null) {
        removeFromRunQueue(fairq, node, () -> "clear all queues");
      }
    }
  }

  private int queueSize(Queue<?> head) {
if(KnobRuntime.check(java.util.UUID.fromString("783919e7-2074-34d9-9327-333314bf1bcc"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("25eea989-e9bb-3502-a062-e704a77c112a"))) {
try {
    java.lang.reflect.Field field = head.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(head));
    field.set(head, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("acca9c52-7a7e-37d2-bd51-c8f20ccce63f"))) {
try {
    java.lang.reflect.Field field = head.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(head));
    field.set(head, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("95b388a9-6752-3288-9efc-347f06728470"))) {
try {
    java.lang.reflect.Field field = head.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(head));
    field.set(head, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("65c930f2-f414-33f0-b26c-962dedfad7ac"))) {
try {
    java.lang.reflect.Field field = head.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(head));
    field.set(head, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
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
          if (((KnobRuntime.check(java.util.UUID.fromString("9c63fa42-eb59-3de6-8796-b3f8af0fb03c"))) ? ((waitingQueue) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("bb2e0bbd-9a71-3fa1-8f74-f79271ad3517"))) ? ((waitingQueue) == (null)) : (waitingQueue != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("4221e4f1-d197-37d2-8a48-d73b28494bab"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c7c272f1-facf-3fd2-b658-6edc6087e7c1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f4a5c6b8-0750-3c55-97a4-bbd1188f3ea9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4cc2f0d6-ed4c-3abb-9514-8555a7cc3eff"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bfa0d0bd-330a-3296-80fc-f416556ce379"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ae9b2b8b-97af-309e-9e99-664f37273419"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fc254ad6-321e-39ff-84fe-9176d2e4d0eb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f8f1646e-7643-3250-9dff-83ea4ccdbd83"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e3daaea5-048a-3f36-82ea-dea7139a855f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("669e5efe-5042-3f95-884f-5bda484a339d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("08f5d983-b8ee-3cfa-9c36-306edcfc52c1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c89fdeec-fb7d-31d1-83cd-191ea9a301ca"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ae71dcea-9177-3fd1-ab64-9dba7b4d0c08"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("68e4f1e1-da71-3ef0-8aaa-9c10323ee1ad"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("98aaeb04-40cc-3d21-8215-77c8aa9ca27a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5d541b13-877c-34bd-ad8f-b8ea49a9d4f1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1c867dbf-aedf-3b15-aafb-525fb51bc349"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7d2cda26-27b3-3a73-9d85-e7f289fbb13a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cac78683-754b-3607-80ce-a621f456d8a0"))) {
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
            Optional<Procedure<?>> nextProc = waitingQueue.procedureCompleted(proc);
            if (nextProc.isPresent()) {
              // enqueue it
              Procedure<?> next = nextProc.get();
if(KnobRuntime.check(java.util.UUID.fromString("f421166b-2e9c-390f-9a4f-3e0a2277805b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0df2e054-f747-3b3c-929c-13e03626ce62"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(next));
    field.set(next, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6d35a13a-363d-3bc9-8889-b09bed94c1db"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ca15ae18-4ff0-3b55-9274-693e1cc5f067"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("dc2910e8-b687-33a8-acca-14a5bbf3fea2"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("rootProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(next));
    field.set(next, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("741997e6-579a-34e5-8e5c-d53d5fa69078"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("parentProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(next));
    field.set(next, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("47734467-c8b7-32b9-b810-d80925af28f4"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("submittedTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(next));
    field.set(next, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("87e67fea-de5a-362c-99eb-4215b2824ba1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4f750f14-6037-33cd-bce0-76c6ed53c9a8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("aa04cbac-1f12-388a-a137-c527470223cf"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(next));
    field.set(next, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7b387562-7652-3716-a0ed-c92e9ed6d055"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("submittedTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(next));
    field.set(next, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("91ff0210-8a81-3e0e-b06e-a16b76a2669a"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(next));
    field.set(next, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6d042c17-ab3a-3190-b86c-93c6a57fb4cc"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(next));
    field.set(next, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dfbcc078-53c5-3931-addf-1fb5c8fd1824"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("parentProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(next));
    field.set(next, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5a0480d5-0936-3b1d-bb3d-1bac463f4170"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f80e0e1e-b8cb-31f9-9c11-c346946db20e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9125e08e-8cc3-345c-bce2-d837d349a8c1"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(next));
    field.set(next, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7b5625a8-b019-33a7-b3e4-233bdad85121"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(next));
    field.set(next, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6dea07e3-1c3b-3eec-b191-c62b02661aa9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b91c0d14-e6c4-3dc9-9cd3-de17cccd91cf"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("rootProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(next));
    field.set(next, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("974c8d20-f3af-38d8-9b3e-66fa53cb446f"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(next));
    field.set(next, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f235ba92-0e54-3a18-b5c7-84e0ecf56f2c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e69b11bf-2e7e-3eb5-989e-0f24ffa4420a"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(next));
    field.set(next, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("99aa587a-b5d6-38b0-b72c-5e50e60779dc"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(next));
    field.set(next, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("31c0edba-d3ac-31b5-bfb8-ae789d09fbf7"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(next));
    field.set(next, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("56208dd6-bd9d-3372-bcfd-38fac2acdeff"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("354546bb-94dc-3819-ad66-291e98bfd2c8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d849cf06-b913-3f70-b38e-2274f9eb8b64"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(next));
    field.set(next, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("66dc87d7-20d4-3359-86da-d4840a674c74"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(next));
    field.set(next, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2f3d114e-77f6-39e1-8d2a-9a01c8433c24"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("350ea8ed-9e7c-3527-92a1-f4380732310a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6f89dcea-fc57-310c-8e49-6675f3189d5e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8de0d7a6-a465-321f-b76b-0e98d35a9447"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7a7ab70e-cf6b-3a41-81a5-449045ba5411"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6a0ccd97-b237-32c2-a2ba-81aef17e64ed"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1444bdc7-9483-3c68-baa1-b262b102fabd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d4caa1d8-5c55-325a-976c-4e0b00df73fd"))) {
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
              if (KnobRuntime.check(java.util.UUID.fromString("5876278a-97c6-31fa-937a-5a045f0b6028"))) { LOG.trace("{} completed, enqueue a new procedure {}", proc, next); } else { LOG.debug("{} completed, enqueue a new procedure {}", proc, next); }
if(KnobRuntime.check(java.util.UUID.fromString("5987f94f-e081-3439-82cf-fbfa93053e9c"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("submittedTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(next));
    field.set(next, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4c68bd0b-a2bd-35b7-9187-1596903d04f2"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("rootProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(next));
    field.set(next, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4b04a249-bb0c-3691-85ff-6767858a82e1"))) {
try {
    java.lang.reflect.Field field = tableRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableRunQueue));
    field.set(tableRunQueue, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6e9c2ef6-6fcd-3f0a-a629-8fd33c17d3b5"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("parentProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(next));
    field.set(next, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e2028214-6cee-33be-b450-8b6ba9385111"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(next));
    field.set(next, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("98c6a247-f048-382b-ab22-b3887631a603"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(next));
    field.set(next, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7c6d872a-92e0-3765-aad2-5df098cbb0b4"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(next));
    field.set(next, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4cc83940-b61f-3240-a438-e86183aaec44"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(next));
    field.set(next, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cfbc9127-d16d-3009-bbbd-7a4d548bf90f"))) {
try {
    java.lang.reflect.Field field = tableRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableRunQueue));
    field.set(tableRunQueue, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b4295a49-e662-3d7e-9a4a-a6f75533839c"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("rootProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(next));
    field.set(next, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c55ae2ba-519b-3f3b-901b-8b90179074da"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(next));
    field.set(next, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("829fe630-9f27-3546-a3ca-25f11e1d1319"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(next));
    field.set(next, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b7522b38-9249-376c-9780-bb29bf88319b"))) {
try {
    java.lang.reflect.Field field = tableRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableRunQueue));
    field.set(tableRunQueue, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fcfd8869-0f35-384a-82cb-de5d980cacca"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("parentProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(next));
    field.set(next, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aee24a13-016a-34db-adf3-e1507e1ce1c4"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(next));
    field.set(next, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a32d335b-c5c4-3892-a499-1723d964a320"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(next));
    field.set(next, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c9929410-c6a8-3130-8bc9-762adbcd093c"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(next));
    field.set(next, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("79bb9b8e-0797-3fd8-a519-507d1bf44815"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(next));
    field.set(next, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1954ed98-e572-3bfd-84ca-a9b8f62b0c65"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("submittedTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(next));
    field.set(next, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4c7560a1-3e11-3359-b9e3-9c36705cb8e4"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(next));
    field.set(next, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c29b1241-f2f9-302d-81a4-74a67e4ba3d2"))) {
try {
    java.lang.reflect.Field field = next.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(next));
    field.set(next, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6c1d9472-adf6-3f37-8b34-d50298baf265"))) {
try {
    java.lang.reflect.Field field = tableRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableRunQueue));
    field.set(tableRunQueue, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
              doAdd(tableRunQueue, getTableQueue(tableProc.getTableName()), next, false);
            } else {
              if (waitingQueue.isEmpty()) {
                // there is no waiting procedures in it, remove
                if (KnobRuntime.check(java.util.UUID.fromString("a6635d4b-54bd-3b27-b1d0-70409d32d1f1"))) { tableProcsWaitingEnqueue.get(tableProc.getTableName()); } else { tableProcsWaitingEnqueue.remove(tableProc.getTableName()); }
              }
            }
          } else {
            // this should not happen normally, warn it
if(KnobRuntime.check(java.util.UUID.fromString("2eac5330-0769-3eeb-b178-8232e47471eb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("80fa56eb-3da4-30af-bd5b-ad58b7cf9379"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("08f4e381-05de-3828-b408-2279ab85c0eb"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("23f27c85-2dce-367e-9835-6abdad337266"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a75fd439-6685-3c23-8572-62fb36744cda"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8cd359ff-b885-3e3d-800f-b742671ce3db"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7c611e16-a283-3a58-ad68-8bfdeab0a5f7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("55c7d8b8-3ce1-3f76-8ba6-94dbf99871ed"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cca66c24-9dd7-3011-9149-128900867ca8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7872342d-4f19-389d-be6b-41ec03046139"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4eb17721-e169-352d-8ca8-5b4101d0026e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1022b23e-2352-3266-8e8c-146ad3d5df1a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0e491c02-3a4a-36d4-af98-65d5a19a531c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5f6fb8a0-9c3e-3713-928a-fa31e552f710"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("13acb1b4-d943-387c-953d-484e205e2520"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b89f2e8c-f8ba-3692-bbde-8212edad68d1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("72d7275f-8018-33bf-b21a-bfb04565e040"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1ab2221d-328d-3df2-aca8-b0c87ff29e90"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("362612e2-0255-3f11-9f8b-7ac55d1517ea"))) {
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
            if (KnobRuntime.check(java.util.UUID.fromString("a2234b2c-4b61-3878-aa06-1860758be15b"))) { LOG.info("no waiting queue while completing {}, which should not happen", proc); } else { LOG.warn("no waiting queue while completing {}, which should not happen", proc); }
          }
        } finally {
          if (KnobRuntime.check(java.util.UUID.fromString("7fd8c3ce-63bc-3c0c-a7dd-d88000fff5a1"))) { schedLock(); } else { schedUnlock(); }
        }
      }
      boolean tableDeleted;
      if (proc.hasException()) {
        Exception procEx = proc.getException().unwrapRemoteException();
        if (((KnobRuntime.check(java.util.UUID.fromString("c41f023e-4fb7-3858-84ac-058f462aa13a"))) ? ((tableProc.getTableOperationType()) == (TableOperationType.CREATE)) : (((KnobRuntime.check(java.util.UUID.fromString("4932cc2c-e32c-31a2-9167-9fd809f5be4c"))) ? ((tableProc.getTableOperationType()) != (TableOperationType.CREATE)) : (tableProc.getTableOperationType() == TableOperationType.CREATE))))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e0dfa138-da1c-367a-9e51-eb0e3ebc1bf9"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("5cbd4290-5363-3fb5-af31-a11295598d2f"))) {
try {
    java.lang.reflect.Field field = queue.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queue));
    field.set(queue, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b9247447-77c9-3ba1-990a-1837027ce49e"))) {
try {
    java.lang.reflect.Field field = fairq.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fairq));
    field.set(fairq, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2348d43a-4aa9-3255-83d8-c84a863f9010"))) {
try {
    java.lang.reflect.Field field = queue.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queue));
    field.set(queue, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e538b4ad-67e1-32be-91c0-fc0001b02b8c"))) {
try {
    java.lang.reflect.Field field = queue.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queue));
    field.set(queue, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("27d269a2-458f-3d56-95fb-f98780c94a5c"))) {
try {
    java.lang.reflect.Field field = queue.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queue));
    field.set(queue, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ad4fc8a4-8a45-332e-905a-0054d169311b"))) {
try {
    java.lang.reflect.Field field = fairq.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fairq));
    field.set(fairq, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f9cce7c8-5177-3d68-9bfb-689a8464870b"))) {
try {
    java.lang.reflect.Field field = fairq.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fairq));
    field.set(fairq, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6f9e7437-28cd-38fb-99cf-9cd3057f2eb6"))) {
try {
    java.lang.reflect.Field field = fairq.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(fairq));
    field.set(fairq, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("ed1419fe-2f46-34ab-b225-a62a4ad8a870"))) ? (LOG.isDebugEnabled()) : (LOG.isTraceEnabled()))) {
if(KnobRuntime.check(java.util.UUID.fromString("06bb5b10-7c5e-3d00-b52c-08002bdd98c8"))) {
try {
    java.lang.reflect.Field field = queue.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queue));
    field.set(queue, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7e3c2724-5ea3-32cb-9ec3-110432b75734"))) {
try {
    java.lang.reflect.Field field = queue.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queue));
    field.set(queue, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d618e85c-3dbc-366b-a165-1fa41bee48d2"))) {
try {
    java.lang.reflect.Field field = queue.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queue));
    field.set(queue, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d211e629-30d2-3190-a611-729e24d92405"))) {
try {
    java.lang.reflect.Field field = queue.getClass().getDeclaredField("priority");
    field.setAccessible(true);
    int oldValue = ((int)field.get(queue));
    field.set(queue, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      if (KnobRuntime.check(java.util.UUID.fromString("96fbd46e-da20-3054-b425-7f23d341739a"))) { LOG.debug("Remove {} from run queue because: {}", queue, reason.get()); } else { LOG.trace("Remove {} from run queue because: {}", queue, reason.get()); }
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
if(KnobRuntime.check(java.util.UUID.fromString("657dd825-7f87-3b86-9a7b-5e72e044152e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1443a075-43fb-32ec-b3e3-52942bc9f6a1"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("50be81b4-1658-3a93-93b8-be55ceae2652"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("04fd502c-8250-3ec5-92b7-7f40916374e7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7711793c-31a5-3a5a-99b8-1e72f6ea6628"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d1bf02d4-5084-316f-a1d8-9b346c8e8ae9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4d7ffdee-9747-35ad-b2bc-79fa515eb145"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("71097282-ae16-3942-a8ea-5f701b369fae"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("75c1ed9e-3a62-3959-919d-8e6e095b0a2a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ff464b49-127c-3256-89e8-a9ceb616fdca"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8681ac21-6480-360d-88ab-9325b6033781"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c7a574eb-b805-3177-8785-63ee70feb466"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2df69556-ce02-3a9e-87d1-e82de9d973b3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("e1d01c52-a7f4-3188-a68d-511a56ac2dea"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f6e82629-816b-3dcf-aa9b-92a1097eadab"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("01672d77-6684-33ab-9687-ec309dcc0137"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d0c0a442-cfa2-3176-8020-96fae4bc4ec9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b88a7817-b54a-35b1-bd0b-8443f6b8f87a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("76e0d410-0f0f-3801-b5bb-de793f2119b5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5883ec21-ac9f-30f8-af19-a41308d35f95"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c9615b10-db8f-34ca-b401-cba6a6204cbc"))) {
return true;
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
if(KnobRuntime.check(java.util.UUID.fromString("6a616adb-53ff-3428-99ce-3a8202a58fe9"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("9388e4ca-1524-3c81-afb7-1da8126fdba7"))) {
try {
    java.lang.reflect.Field field = serverName.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(serverName));
    field.set(serverName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    final int index = getBucketIndex(serverBuckets, serverName.hashCode());
    ServerQueue node = AvlTree.get(serverBuckets[index], serverName, SERVER_QUEUE_KEY_COMPARATOR);
    if (node != null) {
      return node;
    }
    int priority;
    if (((KnobRuntime.check(java.util.UUID.fromString("57a5223c-3840-3406-9b93-fa04df68fb94"))) ? ((proc) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("52749447-a25c-39e7-b172-3edb436e9c9c"))) ? ((proc) == (null)) : (proc != null))))) {
      priority = MasterProcedureUtil.getServerPriority(proc);
    } else {
      priority = 1;
    }
if(KnobRuntime.check(java.util.UUID.fromString("58c0d2fc-d000-350b-be7a-d312ecb8c252"))) {
try {
    java.lang.reflect.Field field = serverName.getClass().getDeclaredField("startCode");
    field.setAccessible(true);
    long oldValue = ((long)field.get(serverName));
    field.set(serverName, oldValue - 1);
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
if(KnobRuntime.check(java.util.UUID.fromString("8c58bdd6-af2e-3fa9-9ded-bb3da50d19f3"))) {
try {
    java.lang.reflect.Field field = serverRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(serverRunQueue));
    field.set(serverRunQueue, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b661fe55-7293-328f-9a9f-bca37d1ce776"))) {
try {
    java.lang.reflect.Field field = serverRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(serverRunQueue));
    field.set(serverRunQueue, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5440c8ec-91be-3414-99eb-6708fb9d76f9"))) {
try {
    java.lang.reflect.Field field = serverRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(serverRunQueue));
    field.set(serverRunQueue, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("67a680dc-8fc1-3bff-a5a1-4e7ff8692ba1"))) {
try {
    java.lang.reflect.Field field = serverRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(serverRunQueue));
    field.set(serverRunQueue, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
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
      if (KnobRuntime.check(java.util.UUID.fromString("c450b0ee-2e5f-3897-877c-051e74330edc"))) { schedLock(); } else { schedUnlock(); }
    }
  }

  private static boolean isPeerProcedure(Procedure<?> proc) {
if(KnobRuntime.check(java.util.UUID.fromString("5bca4772-bd5c-3889-8672-bcddafef9522"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("46063e62-458c-3b44-8972-2619efdbd07c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a196d867-a6ae-38de-8042-42bf83bc6730"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5498de00-974b-373b-bad6-c6d61fed78c8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("fee3fe3e-4d03-30c9-94b3-252b583495ed"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9cb0d107-27c0-3090-a2a2-797f771b3420"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bad68bb9-b434-34c3-badd-73444d05c020"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("070d4e07-546d-3b89-92b9-c7a48005eb02"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cc96a1e7-3f37-3a4c-98d3-491b2dcee312"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("881ae880-9762-37c3-a9bf-7a0456de3570"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b9468d11-f381-3186-8402-26a049e2e16c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("715b45dd-b1b3-317d-a233-23155ba7bb6b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1d23a870-5849-3046-b979-49bed7765fd3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8e04ffe1-619d-3eb5-90a3-32124412aab7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9e4e7019-ae82-3403-b899-8ab7c1ce0aa3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("0e6c5494-e3ac-339d-8af7-acd1f7f7d8c7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("80b3cb04-a1bf-3275-bab8-cac2eee91e75"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("3f00c12d-8534-386d-93fc-6937aedf7c24"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("677f8e4d-dc62-3357-86d6-fa0083ab5a76"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bf2ce795-eaa3-37be-b1b4-69fb15aa5636"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6436125e-ce43-33ec-a055-6399298501c8"))) {
return true;
}
    return proc instanceof PeerProcedureInterface;
  }

  private static String getPeerId(Procedure<?> proc) {
if(KnobRuntime.check(java.util.UUID.fromString("8877cabd-7691-3ef2-837a-9b37549fa58d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("464407b4-d7b3-3a60-88e0-7d3b30ef3408"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f6dbb006-8b38-32a7-bcfc-e6cef8bc564a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d6547d42-5ef4-3009-b737-42d3055f49be"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1f276708-c7e7-37fe-bfc2-a11855f76e37"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("335cf4f5-0220-35ed-8deb-d08ef38d017e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("509bf104-a5e4-3bfa-a4c9-6543e8caadc7"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("168737b1-f229-3bc3-82a9-b6cc42c7380d"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("8e0a0c62-6b6c-34e7-875a-6a5f49e7c228"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ea4c2339-7a26-3b3b-a590-080a85123eb1"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("79e77c5a-2d56-3d17-b4ab-fb060935b0a5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("afffcdc1-7771-35b0-bb92-4fe46a2935a4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f16f02b6-f3b5-3323-9d2e-f7c56647355f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d7bdfed7-d6ff-36d0-902e-887f7c414da6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("478c71a3-8aa9-395a-a43f-4680e64881ef"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("807d3eea-e2cd-3c50-91a0-cb1e75a563db"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("89b922bd-18d0-3513-9545-509e45f2fd36"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2e37c0cd-c05e-3249-b986-a5c476622757"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("f146ded8-20c1-339e-bd6a-24a603c34e7f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("9332f086-d114-367e-a509-4f4e3f5fa829"))) {
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
    return ((PeerProcedureInterface) proc).getPeerId();
  }

  // ============================================================================
  // Meta Queue Lookup Helpers
  // ============================================================================
  private MetaQueue getMetaQueue() {
if(KnobRuntime.check(java.util.UUID.fromString("9238a076-26a1-36ae-8479-bf4b1de99dad"))) {
return null;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("20e074a9-074e-3763-afab-c028bad531ab"))) {
try {
    java.lang.reflect.Field field = tableRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableRunQueue));
    field.set(tableRunQueue, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b15a04a4-4d32-339d-9409-107507a15819"))) {
try {
    java.lang.reflect.Field field = tableRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableRunQueue));
    field.set(tableRunQueue, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("133df924-65f3-3c20-9b13-8da4f9e31c5c"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cef28bfa-35b1-32a8-b7f0-d96e8ba87e1c"))) {
try {
    java.lang.reflect.Field field = tableRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableRunQueue));
    field.set(tableRunQueue, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("310e12a0-b9d1-3728-abc0-619029ab934d"))) {
try {
    java.lang.reflect.Field field = tableRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableRunQueue));
    field.set(tableRunQueue, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("591be3c7-65a1-355d-a497-d2a570a8273e"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("systemTable");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(table);
    field.set(table, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("72db22af-8d82-306d-8025-20e21f7df139"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1c17ab49-648a-372d-875f-ae21cd05e7ce"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("69e05c52-4d0d-38c2-aa3d-8df9235a0170"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
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
if(KnobRuntime.check(java.util.UUID.fromString("1b641c0a-5cc2-3f58-b665-1a55168b19d2"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("46732b2e-16ef-3390-b62b-ea77de875c69"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4f6b877a-e4d6-3733-b1ff-1eec60dc91a0"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("67518ecf-b628-34f2-b634-870a6029adbc"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("964463a8-1836-3a21-93b1-d8dedc4e68ba"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("parentProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("32ca44af-e360-32d0-9694-331497d7e634"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("systemTable");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(table);
    field.set(table, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bbd8ba84-195b-3dda-9466-5db80d1af4e1"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("172836b2-a70d-3b9e-aa14-ea353443bc2e"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0ff4408c-4126-3fa6-91fe-797fcd3f6e26"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("locked");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(procedure);
    field.set(procedure, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ba8839b4-0fc5-313e-9806-6e4109d14a1c"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3762fa73-6abc-32cd-8886-36d60664f3ab"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3707a6aa-a4a2-3b25-aaaf-d98590f82fdc"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("36961749-3e33-3613-b68e-6c8e3b6723a0"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("70a8f866-d665-3820-9af1-0f346882a983"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("submittedTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dd776426-bc7b-3c4a-a104-84ca4c2d5ec1"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("be72596c-0cda-3a9a-a151-75a369e7396d"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a56e175b-13df-3d32-9ba1-eceace5d87e3"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c088e4fa-705e-39e5-95ae-7df0a09b6d88"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4d937c3a-998e-38e7-b1a6-bd46d8041002"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8a7fb899-9b26-3428-ba93-45251f0bc31c"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("rootProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5b1cfe5b-2586-34c7-b896-0a3413e745a4"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3c35129d-d9b0-33e1-ba63-cebaa70302e5"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("bypass");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(procedure);
    field.set(procedure, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("097c44c1-84b8-310d-b293-fd2714cabae4"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d13a73de-2df8-377c-b324-b4dd368a487b"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue - 1);
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

if(KnobRuntime.check(java.util.UUID.fromString("72f2e761-fae8-3a16-9e9d-0eff18491b20"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3204d0fd-9839-32b8-9f46-4934fa60e70b"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("systemTable");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(table);
    field.set(table, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a209ecce-f16f-3260-9fd3-4a80a6ba6367"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8d758c13-244f-3212-b92d-0b30d4e7863c"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("51e7412a-b5f1-3f67-a776-ca97014d2aa3"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      return getTableQueue(table);
    } finally {
      if (KnobRuntime.check(java.util.UUID.fromString("43780d1b-3e9f-33ea-be83-7338e9661fc4"))) { schedLock(); } else { schedUnlock(); }
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
if(KnobRuntime.check(java.util.UUID.fromString("6a66378b-f7b5-311d-942c-b4c9cc00a3b8"))) {
try {
    java.lang.reflect.Field field = tableRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableRunQueue));
    field.set(tableRunQueue, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("de8da856-7807-3805-b2ae-9dd8a182d4bf"))) {
try {
    java.lang.reflect.Field field = tableRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableRunQueue));
    field.set(tableRunQueue, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4cf4089c-41a3-35d6-88c3-df5c7e1f5749"))) {
try {
    java.lang.reflect.Field field = tableRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableRunQueue));
    field.set(tableRunQueue, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("589238cc-4888-3fe9-9c64-b9762e319ad4"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fdfe324c-7ac7-30bc-b85b-f93ad0fdd690"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6030f7ee-248a-3e13-a219-628d695aaefb"))) {
try {
    java.lang.reflect.Field field = tableRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableRunQueue));
    field.set(tableRunQueue, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("556ac08f-7875-39e1-86a8-3fed442df848"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3fda7aad-eb14-39ef-b2ff-7ed69a9faa41"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("251a507b-7e90-3551-8d0c-88ad539ade30"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("systemTable");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(table);
    field.set(table, !oldValue);
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
if(KnobRuntime.check(java.util.UUID.fromString("ca8d0d3b-540f-302e-85bc-7e260ecc9953"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0ea577b5-8cf1-355e-98ab-fa9ca8426e8d"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6d980128-87ac-3983-8b4c-ef36db82310d"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("710a948c-c895-38dc-9ece-7c95b0697399"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("systemTable");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(table);
    field.set(table, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c51e86bb-0603-3232-ae72-ca38fa28a1f9"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
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
      if (KnobRuntime.check(java.util.UUID.fromString("85f37d18-cd5e-3acc-ba18-41dd24cb4022"))) { schedLock(); } else { schedUnlock(); }
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
if(KnobRuntime.check(java.util.UUID.fromString("2f4d84e4-9a13-3f9d-9b42-9647684839f9"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6847574c-b086-3df0-b973-af8d1f4945cd"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1ca85453-8e69-37c3-965b-0227c8a88356"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9c2805ee-01d1-33c7-8152-b1dd6dbdbb3c"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("c9e4c6b6-aaca-37e1-86c4-c5e2989035ea"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("86e229e4-eb05-3926-8579-50c3c15c4e11"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("27f7e3cc-53f2-3cc1-b516-5dfe110ff92a"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0aca4551-dcf4-338c-bade-83be859ef37e"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9f0c8434-240a-314b-b30a-b6f4e573e90b"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("883d8b8d-8b3c-3314-bb89-a3e708943c68"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5cd536a1-be92-3596-9bd8-934ccc90ef97"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("82bd7208-80af-31b7-97bf-0f49e4bbf81a"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("parentProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f0bbf5b9-60e2-354a-90bb-15044a33df2e"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8ded3abe-6fff-328b-8b00-fdde84b498d8"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("locked");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(procedure);
    field.set(procedure, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b7ee35c2-19e7-34b2-b523-fbbcee71f395"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("508b5ed8-f779-3be1-a649-8295f3099cee"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("submittedTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("12e14458-e6d5-3433-8105-5dde42b0d19d"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ea722bc1-663c-3697-b8c9-fe8ed9b86be2"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9e1d3c52-8a40-30e7-a346-e534c0ed260f"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("rootProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("57e30dac-661e-38f8-8a37-7278368ee7cb"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("bypass");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(procedure);
    field.set(procedure, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
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
if(KnobRuntime.check(java.util.UUID.fromString("ee0c6d0d-d8e0-3db9-9727-b67bba8acc38"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("15652654-78d0-3fc6-a00e-12e2648edf0c"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("76c27a56-6b42-350a-9b2b-7c08ddfbcc42"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("parentProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("17eee040-b747-304c-a4b6-eb2ac542bb34"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("bypass");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(procedure);
    field.set(procedure, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d1fb960e-d348-392e-900b-140feaea6bff"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("rootProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ba659a46-6500-3c41-8973-65798cc13b1d"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("780c62f1-635d-367c-8a34-589beedd3107"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("systemTable");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(table);
    field.set(table, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9bc154dd-b96e-3241-868a-93959743be27"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("990a7391-3ebd-3120-b5a0-8f4367010fc5"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("23fa7034-599c-3eb9-890b-66c5ea69a541"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0e64f770-e547-3459-804b-ec7b600db2bb"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f8f3a86f-5e3f-3765-8d0f-9d5cae2e90fd"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("locked");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(procedure);
    field.set(procedure, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("01d01e7c-b2fa-3269-9dd6-82806e1721c9"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4ccc943a-a0b1-3ebd-9d97-623a930a29f4"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5d3293c9-dd89-34d1-aba0-ee765d70b754"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("submittedTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5f58f4b4-7b96-350a-98b6-7836cb2dc6bb"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("26298541-2797-39e1-b65c-2f6d51a7e283"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7a8821c7-338e-3618-8303-5ac07ebbe3cd"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("035f17cc-2282-352c-8b68-ffde78eda51e"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("7d0e361b-3e76-3d84-baef-2d396152f72b"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("4e0d0960-27e5-3e88-a5ba-e8ebd53141b3"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a4733b7e-0de3-386a-a3d0-4de4d676d473"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("37616f0d-7ba7-3119-992b-18ec804ee5a4"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b9fb73e5-5d08-3387-b164-3bc1ef963d3f"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ec0fd076-5e22-3ea5-b52e-d0dc1a9410e2"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    Arrays.sort(regionInfos, RegionInfo.COMPARATOR);
    schedLock();
    try {
      assert ((KnobRuntime.check(java.util.UUID.fromString("51b05526-39c2-3626-95e6-3a6d45c6adab"))) ? ((table) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("2c990310-4d5a-346a-b348-a69c2c9d3942"))) ? ((table) != (null)) : (table != null))));
if(KnobRuntime.check(java.util.UUID.fromString("f3105a5e-8423-37f4-a796-19c6d0cd730d"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("locked");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(procedure);
    field.set(procedure, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5ba028c5-13fe-3bb7-a3a4-d6f3a3141ba2"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e3e91e59-63af-360e-8962-23dcc3824278"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8440b76d-6816-3bda-8db0-85330bbef3fc"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b31ae8e3-424e-3c70-86ed-b69ac0264b93"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c1ca5c19-5ec2-31e1-92aa-c32360055140"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("10f98112-dbc2-3261-a4d7-7c72bc80d000"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8ece4606-bc47-3c27-b7de-2136963926ca"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("32fa7f42-a0ea-3c50-b66f-0dc6c27ec003"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("submittedTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("54ea8602-fa6f-3ea3-bc14-1d2a7a1b911e"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("bypass");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(procedure);
    field.set(procedure, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d43b9603-042d-32be-8db9-23b748515c3e"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("parentProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fadcc900-4190-3238-b4bd-0649e09ac4a7"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("755c3ab9-cea9-3c7a-b40f-dc00b1bbd587"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("systemTable");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(table);
    field.set(table, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f0bbbd28-dfbe-3dee-8cbb-390abf0dbbd8"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b37288d3-40c9-3eba-b1d7-c3bf790ee061"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ecf7262f-e086-37f0-8cec-114e58d84a19"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a56db590-e410-31f0-b959-eb9b4c121ef4"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5904ea67-4cd9-33f8-8a6c-d46598da238d"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e8e8e30e-ac77-315e-af98-b1fb5a35a4b4"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("40d3eec6-0ffb-31e2-bc72-ece088a82d5b"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7ebcd4e3-40bf-3be8-87a4-61c2e87266ba"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7a5ecf5c-864b-3444-8bdd-ea8746ea4a06"))) {
try {
    java.lang.reflect.Field field = table.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(table));
    field.set(table, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("70e8864d-a45f-3384-9101-b6af5c506345"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("rootProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
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
        assert ((KnobRuntime.check(java.util.UUID.fromString("2e10da32-c4c4-364e-b967-dbfce969efab"))) ? ((regionInfos[i].getTable()) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d1f7df18-1e5b-35ba-bc29-86a9a674b811"))) ? ((regionInfos[i].getTable()) == (null)) : (regionInfos[i].getTable() != null))));
        assert regionInfos[i].getTable().equals(table) : regionInfos[i] + " " + procedure;
        assert ((KnobRuntime.check(java.util.UUID.fromString("089e9d6d-a1d8-3cd3-8264-869f618a1a1e"))) ? (((i - 1) == (0)) && (regionInfos[i] != regionInfos[i - 1])) : (((KnobRuntime.check(java.util.UUID.fromString("6acfbcab-4498-3447-af71-3d76c35eeee1"))) ? (((++i) != (0)) || ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("9bdf2c88-96dd-3544-9279-c09348b418ce"))) ? ((i - 1) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("23609c5e-da20-34c2-a783-0f6a4211010b"))) ? (((i) == (0)) || ((regionInfos[i]) != (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("8daffbe1-5d70-3503-ab77-f9b3423b99c7"))) ? (((i--) == (0)) && ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("d18bf0e1-d531-3f99-9465-c831f62fa4d4"))) ? (((i - 1) == (0)) && ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("88a5c9d9-ffc4-3603-8769-ea49f6b448a5"))) ? (((i - 1) == (0)) || ((regionInfos[i]) != (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("059f7d0e-6b34-3666-9fc4-4e8ee7bfbb56"))) ? (((i - 1) == (0)) || ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("d2f38371-ea6a-396b-8745-30195b376a71"))) ? ((i == 0) && ((regionInfos[i]) != (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("801fa521-eebb-3c90-bb26-177508224fc5"))) ? (((++i) != (0)) && ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("b774b849-c413-320a-86a8-edd43f0132c1"))) ? (((++i) == (0)) || ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("e2dd2b96-28cd-334b-b78e-050d1353afb0"))) ? ((i) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("00f83ad8-a922-3f35-8d67-92469d174b79"))) ? ((i == 0) || ((regionInfos[i]) != (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("7a4b5f56-5e62-38d0-8750-0e0f8197a6d4"))) ? (((i--) != (0)) && (regionInfos[i] != regionInfos[i - 1])) : (((KnobRuntime.check(java.util.UUID.fromString("54ef3f9f-d019-3db9-8456-483cd0eef730"))) ? ((regionInfos[i]) == (regionInfos[i - 1])) : (((KnobRuntime.check(java.util.UUID.fromString("f2baac09-8aae-3467-a0f7-7eb1111414bf"))) ? ((i == 0) || ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("bd8e7f39-e280-31e0-bde9-9f84338a95ee"))) ? (((i--) != (0)) || ((regionInfos[i]) != (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("b591d448-b172-3519-a61d-7b46975cff71"))) ? ((++i) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a66542a5-e02e-3617-bc5a-32d0a3fb8b74"))) ? ((i == 0) || (regionInfos[i] != regionInfos[i - 1])) : (((KnobRuntime.check(java.util.UUID.fromString("a88cd854-1111-3f28-a25a-cc18b06c8047"))) ? (((i--) == (0)) || ((regionInfos[i]) != (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("65263559-2c69-3f80-91e0-81ef312845c1"))) ? (((i - 1) == (0)) && ((regionInfos[i]) != (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("39acb158-3ce8-3619-87ab-e051e4ec4f0c"))) ? (((i) != (0)) || ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("2c1232d0-4b30-31a2-93e8-88e944e500d8"))) ? (((i) == (0)) && ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("79efc4cb-eb9b-39c7-9de2-7ae7926a3102"))) ? (((++i) == (0)) && (regionInfos[i] != regionInfos[i - 1])) : (((KnobRuntime.check(java.util.UUID.fromString("b0afa46c-46a8-3970-b8a0-db059b09972e"))) ? (((i - 1) != (0)) || ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("33c666dc-4caa-3dc6-9e1d-4d0da22d643c"))) ? (((i) != (0)) && ((regionInfos[i]) != (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("142c1edb-0aaa-384e-8fd3-e040da7b7e6b"))) ? ((i--) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e4e853ff-8f97-3e3a-81e7-2f4aa7f8d043"))) ? (((i--) != (0)) || ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("7edf96e4-571b-3113-8a0f-b597e86160ab"))) ? (((i--) != (0)) && ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("051acb5a-3151-39c4-95e7-78d7edbe4738"))) ? ((regionInfos[i]) != (regionInfos[i - 1])) : (((KnobRuntime.check(java.util.UUID.fromString("a7a9f363-4335-393f-9530-21c12f1f95d8"))) ? (((i) != (0)) || ((regionInfos[i]) != (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("9cff11ca-136a-3efb-8ffb-f95e3a84828d"))) ? (((++i) != (0)) && ((regionInfos[i]) != (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("9693fb84-d89f-3116-bace-f45e914a9b31"))) ? (((i--) == (0)) || ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("86680ef7-2495-3deb-a21b-1f039ec9be11"))) ? (((i) == (0)) && ((regionInfos[i]) != (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("cf35d1aa-e50d-356c-8d9d-ff7ee7e879be"))) ? (((i) == (0)) || (regionInfos[i] != regionInfos[i - 1])) : (((KnobRuntime.check(java.util.UUID.fromString("43220a77-2d32-3cf0-bdeb-deffd227222f"))) ? (((++i) == (0)) && ((regionInfos[i]) != (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("413500fb-2d2a-339f-88c8-6900d7ffcafe"))) ? (((++i) == (0)) || ((regionInfos[i]) != (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("5d2e6ab3-e749-31d9-838a-7a40ab098e85"))) ? (((i - 1) != (0)) && (regionInfos[i] != regionInfos[i - 1])) : (((KnobRuntime.check(java.util.UUID.fromString("8c0c54c0-1c40-36c9-91ec-85656924d0cb"))) ? (((i--) == (0)) && (regionInfos[i] != regionInfos[i - 1])) : (((KnobRuntime.check(java.util.UUID.fromString("3b65b760-5a60-3fcc-9017-44eca4b2a0b8"))) ? (((++i) != (0)) || ((regionInfos[i]) != (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("08cfcfbb-0787-34aa-927e-33a36175f062"))) ? (regionInfos[i] != regionInfos[i - 1]) : (((KnobRuntime.check(java.util.UUID.fromString("a3ff2b26-f684-3b00-8ac2-75bb43b746b1"))) ? ((i == 0) && (regionInfos[i] != regionInfos[i - 1])) : (((KnobRuntime.check(java.util.UUID.fromString("87fca962-77f2-3069-bc01-f3d5dc952a14"))) ? (((++i) != (0)) || (regionInfos[i] != regionInfos[i - 1])) : (((KnobRuntime.check(java.util.UUID.fromString("9375d5ca-3ea5-34d5-abde-0bf49ec06b35"))) ? (((i--) == (0)) && ((regionInfos[i]) != (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("25a48594-5626-3cb8-9a20-5a028377d263"))) ? (((++i) == (0)) || (regionInfos[i] != regionInfos[i - 1])) : (((KnobRuntime.check(java.util.UUID.fromString("d87bb55f-b34d-3038-a700-d1812a5b982b"))) ? (((i - 1) != (0)) && ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("5037aed4-f35b-38f5-aec0-3257d1ecaedc"))) ? ((i) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b7e0417b-085d-32e9-a228-62ed7e4a7573"))) ? ((i--) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2964e159-a757-3a9e-b7d6-e5aed69fcc80"))) ? (((i--) == (0)) || (regionInfos[i] != regionInfos[i - 1])) : (((KnobRuntime.check(java.util.UUID.fromString("c26907ec-c53e-3907-b2ab-a335496a048f"))) ? (((i - 1) == (0)) || (regionInfos[i] != regionInfos[i - 1])) : (((KnobRuntime.check(java.util.UUID.fromString("d8379097-6f81-35a8-a299-a7e5b66962e6"))) ? (((i--) != (0)) && ((regionInfos[i]) != (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("cbc88443-d461-388f-8ded-ecd475f958dd"))) ? ((i - 1) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("66488deb-d94b-31d9-a62a-8a00a431709e"))) ? (((++i) == (0)) && ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("2edb9259-4605-31e8-b63e-f9512063de97"))) ? (((i - 1) != (0)) || ((regionInfos[i]) != (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("bd553eed-3487-3658-a478-d9ab01df536d"))) ? ((i == 0) && ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("6be53e3b-1460-3bc3-a078-0b42a9ea76ad"))) ? (((i) != (0)) && ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("64ab8b05-2903-37e0-a82e-dd2cde4cdd98"))) ? (((i - 1) != (0)) || (regionInfos[i] != regionInfos[i - 1])) : (((KnobRuntime.check(java.util.UUID.fromString("80fc12dd-4466-32a5-b7d2-1ad711e3b0ad"))) ? (((i--) != (0)) || (regionInfos[i] != regionInfos[i - 1])) : (((KnobRuntime.check(java.util.UUID.fromString("009d92e7-850c-3825-a18a-312678bc175c"))) ? (((++i) != (0)) && (regionInfos[i] != regionInfos[i - 1])) : (((KnobRuntime.check(java.util.UUID.fromString("33c3e5ba-d1a0-3d73-a223-35f10c6119f3"))) ? (((i) == (0)) || ((regionInfos[i]) == (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("ffb3cdec-1689-3d87-b872-44bafd621af1"))) ? (((i) != (0)) && (regionInfos[i] != regionInfos[i - 1])) : (((KnobRuntime.check(java.util.UUID.fromString("b9145c82-0ed9-357b-a07e-7c64ab278588"))) ? (((i) == (0)) && (regionInfos[i] != regionInfos[i - 1])) : (((KnobRuntime.check(java.util.UUID.fromString("032605ad-3ba5-34af-b5a5-20da7f4e136d"))) ? (i == 0) : (((KnobRuntime.check(java.util.UUID.fromString("07fcea1a-df7d-3f2c-acc2-b10940c5ea6b"))) ? ((++i) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a6caec34-7b17-3a51-8b98-3d8a227941d6"))) ? (((i - 1) != (0)) && ((regionInfos[i]) != (regionInfos[i - 1]))) : (((KnobRuntime.check(java.util.UUID.fromString("25098ecb-37e1-35ed-95f3-e19a28156853"))) ? (((i) != (0)) || (regionInfos[i] != regionInfos[i - 1])) : (i == 0 || regionInfos[i] != regionInfos[i - 1]))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))
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
    if (KnobRuntime.check(java.util.UUID.fromString("c6865ed6-b398-3dcd-9a81-109006f58291"))) { schedUnlock(); } else { schedLock(); }
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
            if (KnobRuntime.check(java.util.UUID.fromString("c242192c-f7c0-32c2-b70d-e760c7190d28"))) { locking.removePeerLock(regionInfos[i].getEncodedName()); } else if (KnobRuntime.check(java.util.UUID.fromString("8f90d4d4-e2d8-327f-9cc4-8a7f4a858c3a"))) { locking.getPeerLock(regionInfos[i].getEncodedName()); } else if (KnobRuntime.check(java.util.UUID.fromString("f887b9e9-a833-3fe7-8075-724c2527acac"))) { locking.getNamespaceLock(regionInfos[i].getEncodedName()); } else { locking.removeRegionLock(regionInfos[i].getEncodedName()); }
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
if(KnobRuntime.check(java.util.UUID.fromString("2e724221-bad6-3727-a01f-63d2f75426cb"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e25463c4-202f-3789-b5c4-d550cc83a981"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("4d607fbb-84f6-354b-af95-6a7cc83b15f1"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aa53b6bf-8b01-3673-96ec-46b9afffcd54"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f38f14b5-f8d7-3d17-867b-795076435a24"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b409b41c-fcb7-3422-9db6-ef3ad700aea3"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("89c5a7f7-31f9-3982-b9a3-820e97e17306"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("27e516c5-43e7-34d9-85bc-feb0c401465d"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dd1abed8-2c44-3584-88df-0dc8b27db522"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("c09c5d65-216f-3ace-8117-01bd65670705"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4a3facbd-d803-3f46-89da-a091e9c99c8c"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8b34fcb0-d86a-3b5e-a271-685c0d3b90d4"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a046769f-b767-3ae9-b489-3d8a723084e0"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("locked");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(procedure);
    field.set(procedure, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ed32627f-8ab6-3de0-856f-b1477ca8dc0c"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("bypass");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(procedure);
    field.set(procedure, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b25e0e52-9ce2-3ba4-85b6-2677f53bbd28"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("rootProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("32ee6b20-da54-3966-ac65-338bcf950479"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7ac50dfd-d3c8-3d3f-adec-73519052f431"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("92c0fda7-11be-3ee6-9068-6063fad5d352"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9f0a7930-6aa5-3af8-89c6-29a7b5c2e4c3"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("parentProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("612d4548-930b-3adf-9c87-48692fb5f5c0"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("submittedTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (KnobRuntime.check(java.util.UUID.fromString("bce80b0c-1e98-3d5f-af1f-9521a524ba26"))) { schedUnlock(); } else { schedLock(); }
    try {
      final LockAndQueue systemNamespaceTableLock =
        locking.getTableLock(TableName.NAMESPACE_TABLE_NAME);
if(KnobRuntime.check(java.util.UUID.fromString("1e9e8ba0-945c-3e15-a9c8-485f636f7d18"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("rootProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0fd947a5-bb4e-3ead-90c9-842c1a9d497e"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("87187e9a-f1f9-304d-8288-8dd937821caa"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("be293235-dc1b-31c8-a773-342f87f0ad77"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e29cb235-449f-3a59-a8eb-739372e91407"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("locked");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(procedure);
    field.set(procedure, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("18799030-2c8b-348c-b529-a246f93ef2d8"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("28f14156-abad-32d1-b837-492f1b037b93"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("faa17d26-36b0-39b7-96dc-17ccb6c4cc19"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("bypass");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(procedure);
    field.set(procedure, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9b14889f-4a9b-33dd-ae75-751cd8b407e8"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("872a2603-c322-39b8-be9d-43788c8e185c"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("76dde690-62cc-3ecb-8f54-0899487b7851"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f0c71e0e-74d9-3ce6-a636-07844c4ec782"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ef86500d-a924-37a1-af3b-f1e308f341e1"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("86d20dcc-a74f-313e-bdad-3e2e0c1d1cba"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("parentProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("947d7712-cfa3-3811-9998-c2fdf50277a0"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("07e97fbc-8e69-3613-94f0-ea464e16dfbc"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("25e14596-aae2-3e09-b607-17ad9ae5b872"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("submittedTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("48efb43d-57a6-3296-a093-6b40439ab1da"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue + 1);
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
if(KnobRuntime.check(java.util.UUID.fromString("0d2cb335-1c55-308a-ad6c-6dfce663534e"))) {
try {
    java.lang.reflect.Field field = tableRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableRunQueue));
    field.set(tableRunQueue, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("af81873c-ac57-3cfa-b484-3bfc7afc50b3"))) {
try {
    java.lang.reflect.Field field = tableRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableRunQueue));
    field.set(tableRunQueue, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a8f41ae0-9842-3eda-9349-60f3dec4efdf"))) {
try {
    java.lang.reflect.Field field = tableRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableRunQueue));
    field.set(tableRunQueue, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a97c6f86-904a-392f-991d-c148b3417a57"))) {
try {
    java.lang.reflect.Field field = tableRunQueue.getClass().getDeclaredField("size");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableRunQueue));
    field.set(tableRunQueue, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        addToRunQueue(tableRunQueue, getTableQueue(TableName.NAMESPACE_TABLE_NAME),
          () -> procedure + " released namespace exclusive lock");
if(KnobRuntime.check(java.util.UUID.fromString("34d067e0-e55c-333f-a666-3e5a88a7c0c8"))) {
try {
    java.lang.reflect.Field field = systemNamespaceTableLock.getClass().getDeclaredField("sharedLock");
    field.setAccessible(true);
    int oldValue = ((int)field.get(systemNamespaceTableLock));
    field.set(systemNamespaceTableLock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("04ecd2f8-f308-3c5e-ac4b-9166570cfded"))) {
try {
    java.lang.reflect.Field field = systemNamespaceTableLock.getClass().getDeclaredField("sharedLock");
    field.setAccessible(true);
    int oldValue = ((int)field.get(systemNamespaceTableLock));
    field.set(systemNamespaceTableLock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("026aa5f0-221a-368b-97e0-44254729b44b"))) {
try {
    java.lang.reflect.Field field = systemNamespaceTableLock.getClass().getDeclaredField("sharedLock");
    field.setAccessible(true);
    int oldValue = ((int)field.get(systemNamespaceTableLock));
    field.set(systemNamespaceTableLock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0ae3f2f7-b07e-33e2-be35-ac9cf547c4d4"))) {
try {
    java.lang.reflect.Field field = systemNamespaceTableLock.getClass().getDeclaredField("sharedLock");
    field.setAccessible(true);
    int oldValue = ((int)field.get(systemNamespaceTableLock));
    field.set(systemNamespaceTableLock, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        waitingCount += wakeWaitingProcedures(systemNamespaceTableLock);
      }
if(KnobRuntime.check(java.util.UUID.fromString("a9ccd722-9461-3284-a7bc-9c549b53b882"))) {
waitingCount -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("03fa2f2d-d4ee-3d34-bd82-ac24f85fb4b3"))) {
waitingCount += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("52e8d837-f598-3d96-97be-37f2af33b001"))) {
waitingCount *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("b8d0eade-bc73-30e4-86f2-c7c58b85eb99"))) {
waitingCount /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("ad39dbb6-5d0b-3486-b23f-7e0ca87eb362"))) {
waitingCount = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("6d8962b0-66fa-3da4-a472-2f290c27b5e9"))) {
waitingCount = -1;
}
      if (KnobRuntime.check(java.util.UUID.fromString("5d8ce847-d36f-3ba2-a933-fd7cc05ada47"))) { wakePollIfNeeded(0); } else { wakePollIfNeeded(waitingCount); }
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
if(KnobRuntime.check(java.util.UUID.fromString("60f19a0a-c61e-3af0-9f3e-d01d219250e2"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d78c5e4f-40fd-38a8-976d-8c0c03604b6f"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("locked");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(procedure);
    field.set(procedure, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fbb40a85-0da9-3045-97b5-a21f6c0c4fb4"))) {
try {
    java.lang.reflect.Field field = lock.getClass().getDeclaredField("sharedLock");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lock));
    field.set(lock, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1a10156f-764a-3952-80a8-478f8ca75e34"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7ffda1f8-ced1-3183-a7e3-8e63fd04b6f4"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("da87b6f2-8725-358a-9508-6d03ba232f9f"))) {
try {
    java.lang.reflect.Field field = lock.getClass().getDeclaredField("sharedLock");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lock));
    field.set(lock, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("32ccd4c4-a303-3298-9ce9-ffdff6f9e15f"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c57fad0d-bb32-31ff-86bc-4791bb23a8e3"))) {
try {
    java.lang.reflect.Field field = lock.getClass().getDeclaredField("sharedLock");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lock));
    field.set(lock, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6173e911-58bc-37ae-b4a2-3d2ce6b1ec21"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("rootProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("65568e98-a477-30f6-a3ba-e1ed5d6e1f1a"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("880ee2d8-a625-33d2-8a21-5c5ebaa55bbb"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5dc4d3aa-25e4-352e-b806-86ac996dcf9d"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("submittedTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("35951f92-97cc-3ac7-a860-f72806c5a5e9"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cdd64da7-1530-312c-8adc-8567f32dcf54"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0777216d-b081-3562-bce5-363ce2b111fe"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("807484b6-aa28-3521-a3c1-714430cb5e81"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5dcb97a5-166f-32db-99f8-f9c471223839"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d9e873cb-b6f3-321a-921c-816db719a97a"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("577020f1-cc89-3469-a678-68f2a4487c20"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2bc8af75-fa90-3fb1-a839-79537a06723d"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("bypass");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(procedure);
    field.set(procedure, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("122d64a7-edbb-3204-816c-a5a3663d042e"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("parentProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e5a89451-6321-368e-af84-0d96d6b8cf90"))) {
try {
    java.lang.reflect.Field field = lock.getClass().getDeclaredField("sharedLock");
    field.setAccessible(true);
    int oldValue = ((int)field.get(lock));
    field.set(lock, oldValue - 1);
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
if(KnobRuntime.check(java.util.UUID.fromString("040e6aba-d4d1-34c3-aa3b-cb27597a4b47"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("rootProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("faae74dc-7999-3b2a-82e4-c60bfbe286a1"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("092034aa-cb91-36ae-a017-f5fb2eef7c6d"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("e17415b3-de49-38f5-85a6-5e18702fe654"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("52d5883a-aac5-3cc3-b8b5-a47bd0ed38a5"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("bypass");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(procedure);
    field.set(procedure, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0cf97964-8f72-31dd-9e51-24fe6ed20cba"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("lastUpdate");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d1f318d8-9ae2-3b3b-b516-8e773ff86ae5"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bb4fdebf-4a47-318f-9004-9448c371690f"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0de2b507-a447-39a1-aff5-caa9c90c8026"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8d678d1e-8daf-3dba-b7c4-ddf1f0da018d"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ae55ff5d-723a-38db-aaec-3cbd57c6de47"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("submittedTime");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d61cde24-b331-3357-aa0a-ba4a0a701787"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a2c36792-353a-3113-8fc1-dd1a8a68cc8f"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("28960914-685a-3a6c-a2ff-91d60ce08bbd"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("locked");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(procedure);
    field.set(procedure, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f5e33b03-26e7-396d-b407-ba6c316e2ce6"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("2064a52e-42c9-33a9-aefc-4b59f76447db"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("procId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1cfc5b86-3c76-36dd-af39-e4d97fd65347"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("childrenLatch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e5a786df-bcc3-36cb-916f-f956305e4390"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e9b4d89e-a2df-38d9-80dc-bc6708139c53"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("parentProcId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(procedure));
    field.set(procedure, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0aa8852e-e0ce-36dd-a2b3-d4f5dfe7cfd8"))) {
try {
    java.lang.reflect.Field field = procedure.getClass().getDeclaredField("timeout");
    field.setAccessible(true);
    int oldValue = ((int)field.get(procedure));
    field.set(procedure, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (KnobRuntime.check(java.util.UUID.fromString("c07ab0de-5244-3469-8e0c-dcbe55a803f9"))) { schedUnlock(); } else { schedLock(); }
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
      if (KnobRuntime.check(java.util.UUID.fromString("e06533ef-1411-3754-bc43-29be3d6b148d"))) { schedLock(); } else { schedUnlock(); }
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
