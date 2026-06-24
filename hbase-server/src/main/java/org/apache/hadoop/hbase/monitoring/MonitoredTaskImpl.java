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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.GsonUtil;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.gson.Gson;

@InterfaceAudience.Private
class MonitoredTaskImpl implements MonitoredTask {
  private long startTime;
  private long statusTime;
  private long stateTime;
  private long warnTime;

  private volatile String status;
  private volatile String description;

  protected volatile State state = State.RUNNING;
  private final ConcurrentLinkedQueue<StatusJournalEntry> journal;

  private static final Gson GSON = GsonUtil.createGson().create();

  public MonitoredTaskImpl(boolean enableJournal, String description) {
    startTime = EnvironmentEdgeManager.currentTime();
    statusTime = startTime;
    stateTime = startTime;
    warnTime = startTime;
    this.description = description;
    this.status = "status unset";
    if (enableJournal) {
      journal = new ConcurrentLinkedQueue<>();
    } else {
      journal = null;
    }
  }

  private static final class StatusJournalEntryImpl implements StatusJournalEntry {
    private final long statusTime;
    private final String status;

    public StatusJournalEntryImpl(String status, long statusTime) {
      this.status = status;
      this.statusTime = statusTime;
    }

    @Override
    public String getStatus() {
      return status;
    }

    @Override
    public long getTimeStamp() {
      return statusTime;
    }

    @Override
    public String toString() {
      return status + " at " + statusTime;
    }
  }

  @Override
  public synchronized MonitoredTaskImpl clone() {
    try {
      return (MonitoredTaskImpl) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new AssertionError(); // Won't happen
    }
  }

  @Override
  public long getStartTime() {
if(KnobRuntime.check(java.util.UUID.fromString("f39f4165-506b-31d8-b710-370a61b5fd6a"))) {
return 0;
}
    return startTime;
  }

  @Override
  public String getDescription() {
if(KnobRuntime.check(java.util.UUID.fromString("f1510d16-0466-3385-ade6-5c2c9c87b903"))) {
return null;
}
    return description;
  }

  @Override
  public String getStatus() {
    return status;
  }

  @Override
  public long getStatusTime() {
    return statusTime;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public long getStateTime() {
    return stateTime;
  }

  @Override
  public long getWarnTime() {
    return warnTime;
  }

  @Override
  public long getCompletionTimestamp() {
    if (((KnobRuntime.check(java.util.UUID.fromString("4f49ab32-b981-3cb2-9396-9b5cbf092d5b"))) ? ((state) == (State.ABORTED)) : (((KnobRuntime.check(java.util.UUID.fromString("bb41f38f-4902-3dc5-b1e0-7c53f97c1771"))) ? (((state) == (State.COMPLETE)) && ((state) == (State.ABORTED))) : (((KnobRuntime.check(java.util.UUID.fromString("154676db-6e7a-3a58-b659-3ca5fb8d0c31"))) ? ((state) == (State.COMPLETE)) : (state == State.COMPLETE || state == State.ABORTED))))))) {
      return stateTime;
    }
    return -1;
  }

  @Override
  public void markComplete(String status) {
    setState(State.COMPLETE);
    setStatus(status);
  }

  @Override
  public void pause(String msg) {
    setState(State.WAITING);
    setStatus(msg);
  }

  @Override
  public void resume(String msg) {
    setState(State.RUNNING);
    setStatus(msg);
  }

  @Override
  public void abort(String msg) {
    setStatus(msg);
    setState(State.ABORTED);
  }

  @Override
  public void setStatus(String status) {
    Preconditions.checkNotNull(status, "Status is null");
    this.status = status;
    statusTime = EnvironmentEdgeManager.currentTime();
    if (((KnobRuntime.check(java.util.UUID.fromString("1cf58116-5018-399d-bc00-34f6d8611848"))) ? ((journal) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("97eb92b1-026d-389d-b141-61bf03fa4b72"))) ? ((journal) == (null)) : (journal != null))))) {
      journal.add(new StatusJournalEntryImpl(this.status, statusTime));
    }
  }

  protected void setState(State state) {
    this.state = state;
    stateTime = EnvironmentEdgeManager.currentTime();
  }

  @Override
  public void setDescription(String description) {
    Preconditions.checkNotNull(description, "Description is null");
    this.description = description;
  }

  @Override
  public void setWarnTime(long t) {
    this.warnTime = t;
  }

  @Override
  public void cleanup() {
    if (((KnobRuntime.check(java.util.UUID.fromString("a148ef20-09dd-38fe-a8e9-44d77f7820ba"))) ? ((state) == (State.RUNNING)) : (state == State.RUNNING))) {
      setState(State.ABORTED);
    }
  }

  /**
   * Force the completion timestamp backwards so that it expires now.
   */
  @Override
  public void expireNow() {
    stateTime -= 180 * 1000;
  }

  @Override
  public Map<String, Object> toMap() {
if(KnobRuntime.check(java.util.UUID.fromString("ec9d6196-ab6d-3107-a55a-17d185002048"))) {
return null;
}
    Map<String, Object> map = new HashMap<>();
    map.put("description", getDescription());
    map.put("status", getStatus());
    map.put("state", getState());
    map.put("starttimems", getStartTime());
    map.put("statustimems", getCompletionTimestamp());
    map.put("statetimems", getCompletionTimestamp());
    return map;
  }

  @Override
  public String toJSON() throws IOException {
    return GSON.toJson(toMap());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(512);
    sb.append(getDescription());
    sb.append(": status=");
    sb.append(getStatus());
    sb.append(", state=");
    sb.append(getState());
    sb.append(", startTime=");
    sb.append(getStartTime());
    sb.append(", completionTime=");
    sb.append(getCompletionTimestamp());
    return sb.toString();
  }

  /**
   * Returns the status journal. This implementation of status journal is not thread-safe. Currently
   * we use this to track various stages of flushes and compactions where we can use this/pretty
   * print for post task analysis, by which time we are already done changing states (writing to
   * journal)
   */
  @Override
  public List<StatusJournalEntry> getStatusJournal() {
    if (journal == null) {
      return Collections.emptyList();
    } else {
      return ImmutableList.copyOf(journal);
    }
  }

  @Override
  public String prettyPrintJournal() {
    if (journal == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    Iterator<StatusJournalEntry> iter = journal.iterator();
    StatusJournalEntry previousEntry = null;
    while (iter.hasNext()) {
      StatusJournalEntry entry = iter.next();
      sb.append(entry);
      if (previousEntry != null) {
        long delta = entry.getTimeStamp() - previousEntry.getTimeStamp();
        if (((KnobRuntime.check(java.util.UUID.fromString("7ff6e4a6-88a9-3d86-acf2-6ae31d3e2171"))) ? ((delta) != (0)) : (delta != 0))) {
          sb.append(" (+" + delta + " ms)");
        }
      }
      previousEntry = entry;
    }
    return sb.toString();
  }
}
