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
    if (((KnobRuntime.check(java.util.UUID.fromString("025a72f0-fcff-30fe-975e-34b0ead60b62"))) ? (!enableJournal) : (enableJournal))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("8d60ca17-a4d9-3373-8e5b-99352f6254f3"))) {
return 0;
}
      return statusTime;
    }

    @Override
    public String toString() {
if(KnobRuntime.check(java.util.UUID.fromString("11ad6bde-f109-3934-a555-448951f614dc"))) {
return null;
}
      return ((KnobRuntime.check(java.util.UUID.fromString("71d992fc-afb3-3547-8b04-1ece0690a2df"))) ? ((status + " at ") + (statusTime)) : (((KnobRuntime.check(java.util.UUID.fromString("521de3ba-e2cb-37e2-a080-ec59ec2a9275"))) ? (((status) + (" at ")) + (statusTime)) : (status + " at " + statusTime))));
    }
  }

  @Override
  public synchronized MonitoredTaskImpl clone() {
    try {
if(KnobRuntime.check(java.util.UUID.fromString("01595dd9-bfc1-3db3-91f2-7de57050e654"))) {
throw new java.lang.CloneNotSupportedException("Injected exception");
}
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
if(KnobRuntime.check(java.util.UUID.fromString("e1082085-6b95-312b-9242-7a3ccc27164d"))) {
return null;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("e6b916c2-bec7-372b-b1a4-94c602d923cc"))) {
return 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("bb41f38f-4902-3dc5-b1e0-7c53f97c1771"))) ? (((state) == (State.COMPLETE)) && ((state) == (State.ABORTED))) : (((KnobRuntime.check(java.util.UUID.fromString("9a58ea39-84d1-3632-b385-44f7e754f951"))) ? ((state == State.COMPLETE) || ((state) != (State.ABORTED))) : (((KnobRuntime.check(java.util.UUID.fromString("30680a87-6ed1-391c-8d3f-fab75777579f"))) ? ((state == State.COMPLETE) || (state == State.ABORTED)) : (((KnobRuntime.check(java.util.UUID.fromString("4f49ab32-b981-3cb2-9396-9b5cbf092d5b"))) ? ((state) == (State.ABORTED)) : (((KnobRuntime.check(java.util.UUID.fromString("d91a541d-c040-3bfc-bbfa-d68973f5cc7c"))) ? (((state) == (State.COMPLETE)) || (state == State.ABORTED)) : (((KnobRuntime.check(java.util.UUID.fromString("d8cf5845-cb79-3108-a6bc-cc8bf88ed681"))) ? (((state) != (State.COMPLETE)) || (state == State.ABORTED)) : (((KnobRuntime.check(java.util.UUID.fromString("a1695638-aa58-366b-92b6-88d5ffb480e6"))) ? (((state) != (State.COMPLETE)) || ((state) != (State.ABORTED))) : (((KnobRuntime.check(java.util.UUID.fromString("452cf4f8-a8fa-3a21-b5f9-ff8644b64c0f"))) ? ((state == State.COMPLETE) && ((state) == (State.ABORTED))) : (((KnobRuntime.check(java.util.UUID.fromString("a07e4231-ce0a-3687-b86d-46b7aa45b5af"))) ? (((state) != (State.COMPLETE)) || ((state) == (State.ABORTED))) : (((KnobRuntime.check(java.util.UUID.fromString("5d842b3d-2fac-3f4c-936e-f3aa9a8c8e57"))) ? (((state) != (State.COMPLETE)) && ((state) != (State.ABORTED))) : (((KnobRuntime.check(java.util.UUID.fromString("ecfd2236-9115-328f-8c22-d06f14217f0b"))) ? ((state == State.COMPLETE) && (state == State.ABORTED)) : (((KnobRuntime.check(java.util.UUID.fromString("31008058-38f0-3752-bffa-b6642ea1e5b6"))) ? (((state) == (State.COMPLETE)) && ((state) != (State.ABORTED))) : (((KnobRuntime.check(java.util.UUID.fromString("ed1e0166-df76-38c8-a38d-dc88597c09aa"))) ? (((state) == (State.COMPLETE)) || ((state) != (State.ABORTED))) : (((KnobRuntime.check(java.util.UUID.fromString("77fd38dd-2b5c-3870-a80f-35a02e646117"))) ? ((state == State.COMPLETE) || ((state) == (State.ABORTED))) : (((KnobRuntime.check(java.util.UUID.fromString("154676db-6e7a-3a58-b659-3ca5fb8d0c31"))) ? ((state) == (State.COMPLETE)) : (((KnobRuntime.check(java.util.UUID.fromString("b97ca599-fb27-3261-8b43-90c551c9a3ad"))) ? (state == State.COMPLETE) : (((KnobRuntime.check(java.util.UUID.fromString("1628a99c-5b1d-36a2-aa7d-9d538d2f5089"))) ? (((state) != (State.COMPLETE)) && (state == State.ABORTED)) : (((KnobRuntime.check(java.util.UUID.fromString("f5f74222-d139-3caa-82e4-4a93f4c44302"))) ? (state == State.ABORTED) : (((KnobRuntime.check(java.util.UUID.fromString("d18d5638-4540-3e03-9f1a-43e95b543c40"))) ? ((state) != (State.COMPLETE)) : (((KnobRuntime.check(java.util.UUID.fromString("a26ab667-b591-3e31-9963-24332d2aa90c"))) ? (((state) != (State.COMPLETE)) && ((state) == (State.ABORTED))) : (((KnobRuntime.check(java.util.UUID.fromString("ee28aedc-400c-375e-a094-7b1ff7c43dc2"))) ? (((state) == (State.COMPLETE)) || ((state) == (State.ABORTED))) : (((KnobRuntime.check(java.util.UUID.fromString("7ed15087-3de3-3c42-a3f8-6813644725d5"))) ? (((state) == (State.COMPLETE)) && (state == State.ABORTED)) : (((KnobRuntime.check(java.util.UUID.fromString("6649527c-8473-3268-9844-0dcc5509fa72"))) ? ((state) != (State.ABORTED)) : (((KnobRuntime.check(java.util.UUID.fromString("c2afa9fb-ecb3-30a4-8872-29c00187fe8e"))) ? ((state == State.COMPLETE) && ((state) != (State.ABORTED))) : (state == State.COMPLETE || state == State.ABORTED))))))))))))))))))))))))))))))))))))))))))))))))) {
      return stateTime;
    }
    return -1;
  }

  @Override
  public void markComplete(String status) {
if(KnobRuntime.check(java.util.UUID.fromString("02557a45-e0ae-3bb1-bfb2-4fe554076e15"))) {
return;
}
    setState(State.COMPLETE);
    setStatus(status);
  }

  @Override
  public void pause(String msg) {
if(KnobRuntime.check(java.util.UUID.fromString("d4554024-36b5-3d32-8983-4efff82ccad1"))) {
return;
}
    setState(State.WAITING);
    setStatus(msg);
  }

  @Override
  public void resume(String msg) {
if(KnobRuntime.check(java.util.UUID.fromString("af91752a-3ab2-3f75-9d14-6d2e2135897b"))) {
return;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("13814718-36ce-383c-9e8d-84e5feace4b0"))) {
return;
}
    Preconditions.checkNotNull(status, "Status is null");
    this.status = status;
    statusTime = EnvironmentEdgeManager.currentTime();
    if (((KnobRuntime.check(java.util.UUID.fromString("1cf58116-5018-399d-bc00-34f6d8611848"))) ? ((journal) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("97eb92b1-026d-389d-b141-61bf03fa4b72"))) ? ((journal) == (null)) : (journal != null))))) {
      journal.add(new StatusJournalEntryImpl(this.status, statusTime));
    }
  }

  protected void setState(State state) {
if(KnobRuntime.check(java.util.UUID.fromString("c5d357d3-1767-3cae-96f3-e019ee59e50f"))) {
return;
}
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
if(KnobRuntime.check(java.util.UUID.fromString("02e92c2f-75cc-354d-9a7a-3c6e20d32448"))) {
return;
}
    this.warnTime = t;
  }

  @Override
  public void cleanup() {
if(KnobRuntime.check(java.util.UUID.fromString("1d6d8c9e-5c5c-365c-85fb-e0b0be2e7c34"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("ca656c24-8553-39aa-9f12-ce9752bf7bd1"))) ? ((state) != (State.RUNNING)) : (((KnobRuntime.check(java.util.UUID.fromString("a148ef20-09dd-38fe-a8e9-44d77f7820ba"))) ? ((state) == (State.RUNNING)) : (state == State.RUNNING))))) {
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
    if (KnobRuntime.check(java.util.UUID.fromString("3d7ed2aa-fd13-3bbc-a50f-4680bf2ad580"))) { map.put("description", getStatus()); } else { map.put("description", getDescription()); }
    if (KnobRuntime.check(java.util.UUID.fromString("41b284c9-3b58-3d2b-a2c9-d718a3343cea"))) { map.put("status", getDescription()); } else { map.put("status", getStatus()); }
    map.put("state", getState());
    if (KnobRuntime.check(java.util.UUID.fromString("a00ae4ba-12ec-33c5-83c5-c528b385036c"))) { map.put("starttimems", EnvironmentEdgeManager.currentTime()); } else { map.put("starttimems", getStartTime()); }
    if (KnobRuntime.check(java.util.UUID.fromString("a65b0f54-1795-3e14-a6dc-4a111b9055a5"))) { map.put("statustimems", EnvironmentEdgeManager.currentTime()); } else { map.put("statustimems", getCompletionTimestamp()); }
    if (KnobRuntime.check(java.util.UUID.fromString("2610ac55-2ae8-3cf1-b882-b06736222ea2"))) { map.put("statetimems", EnvironmentEdgeManager.currentTime()); } else { map.put("statetimems", getCompletionTimestamp()); }
    return map;
  }

  @Override
  public String toJSON() throws IOException {
    return GSON.toJson(toMap());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(512);
    if (KnobRuntime.check(java.util.UUID.fromString("335a3403-285c-39ac-8f0c-bf5c6510b93c"))) { sb.append(getStatus()); } else { sb.append(getDescription()); }
    sb.append(": status=");
    if (KnobRuntime.check(java.util.UUID.fromString("ab1d4325-dcd7-3db2-aa1a-9d0478288d59"))) { sb.append(getDescription()); } else { sb.append(getStatus()); }
    sb.append(", state=");
    sb.append(getState());
    sb.append(", startTime=");
    if (KnobRuntime.check(java.util.UUID.fromString("67055adb-7eb1-3752-8446-86b17b503291"))) { sb.append(EnvironmentEdgeManager.currentTime()); } else { sb.append(getStartTime()); }
    sb.append(", completionTime=");
    if (KnobRuntime.check(java.util.UUID.fromString("aeccfca7-7a3c-376a-a1c5-02a3071eb873"))) { sb.append(EnvironmentEdgeManager.currentTime()); } else { sb.append(getCompletionTimestamp()); }
    return ((KnobRuntime.check(java.util.UUID.fromString("dd0dfd5d-b676-3da9-b6af-a54df6f43948"))) ? (getStatus()) : (((KnobRuntime.check(java.util.UUID.fromString("6454d3ee-2c44-3095-8676-6d1fdc3b93ee"))) ? (getDescription()) : (sb.toString()))));
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
if(KnobRuntime.check(java.util.UUID.fromString("7dec38b2-f1d3-3a51-b76f-cacb15adb937"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("7d0e07a0-df58-388e-aeac-bcb78ef5bbd3"))) ? ((journal) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("942922b8-c638-3568-93c3-532153e9436a"))) ? ((journal) == (null)) : (journal == null))))) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    Iterator<StatusJournalEntry> iter = journal.iterator();
    StatusJournalEntry previousEntry = null;
    while (iter.hasNext()) {
      StatusJournalEntry entry = iter.next();
      sb.append(entry);
      if (((KnobRuntime.check(java.util.UUID.fromString("a75274cf-09cc-3765-b590-398ed280c604"))) ? ((previousEntry) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fd0c11a5-3e51-391b-87c3-a5d1b59410e1"))) ? ((previousEntry) != (null)) : (previousEntry != null))))) {
        long delta = entry.getTimeStamp() - previousEntry.getTimeStamp();
        if (((KnobRuntime.check(java.util.UUID.fromString("7ff6e4a6-88a9-3d86-acf2-6ae31d3e2171"))) ? ((delta) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f5a644ba-e2a7-313d-ae42-bbe02da83529"))) ? ((delta) == (0)) : (delta != 0))))) {
          if (KnobRuntime.check(java.util.UUID.fromString("5bf18b73-4baa-32b4-b505-25843c9f2bbc"))) { sb.append(((" (+") + (delta)) + (" ms)")); } else if (KnobRuntime.check(java.util.UUID.fromString("a051263e-42ce-37a2-9937-f0e08749c280"))) { sb.append(" (+" + delta); } else if (KnobRuntime.check(java.util.UUID.fromString("e9ef7efa-3b73-36a8-abfe-74c35b0c91fc"))) { sb.append((" (+" + delta) + (" ms)")); } else if (KnobRuntime.check(java.util.UUID.fromString("5be4f27c-80b1-37ee-92e6-b6ecb64a8135"))) { sb.append(" ms)"); } else if (KnobRuntime.check(java.util.UUID.fromString("1a15b938-d715-3e71-89e3-fab2466d0ffe"))) { sb.append((" (+") + (delta)); } else if (KnobRuntime.check(java.util.UUID.fromString("8a329865-f34e-33aa-881b-9523788e6722"))) { sb.append(" (+"); } else if (KnobRuntime.check(java.util.UUID.fromString("9d4f5a47-457d-3b5e-a709-16e7401a2565"))) { sb.append((" (+") + (" ms)")); } else { sb.append(" (+" + delta + " ms)"); }
        }
      }
      previousEntry = entry;
    }
    return ((KnobRuntime.check(java.util.UUID.fromString("c35ee4e1-0e4d-3d1a-a076-90fd5b28c444"))) ? (getStatus()) : (((KnobRuntime.check(java.util.UUID.fromString("56394b41-dc03-3177-89c8-be40aebec3c1"))) ? (getDescription()) : (sb.toString()))));
  }
}
