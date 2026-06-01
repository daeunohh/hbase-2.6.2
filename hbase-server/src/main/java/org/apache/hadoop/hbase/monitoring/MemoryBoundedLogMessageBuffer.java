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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Charsets;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * A size-bounded repository of alerts, which are kept in a linked list. Alerts can be added, and
 * they will automatically be removed one by one when the specified heap usage is exhausted.
 */
@InterfaceAudience.Private
public class MemoryBoundedLogMessageBuffer {
  private final long maxSizeBytes;
  private long usage = 0;
  private LinkedList<LogMessage> messages;

  public MemoryBoundedLogMessageBuffer(long maxSizeBytes) {
    if (KnobRuntime.check(java.util.UUID.fromString("da653f1b-7217-3c29-b3a5-af77dbe1585a"))) { Preconditions.checkArgument((maxSizeBytes) >= (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("d484b729-70dc-3f23-b2c4-d11ee062398d"))) { Preconditions.checkArgument((maxSizeBytes) <= (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("f52aaf92-7a8d-303e-a4ae-edf088b08af0"))) { Preconditions.checkArgument((maxSizeBytes) == (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("0def594d-4740-31e8-b25b-3971885f5eaa"))) { Preconditions.checkArgument((maxSizeBytes) < (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("986c10a1-a419-3195-b01b-d07632b808d0"))) { Preconditions.checkArgument((maxSizeBytes) > (0)); } else if (KnobRuntime.check(java.util.UUID.fromString("1df4bdd1-f5b3-3890-9512-2c0def4022f6"))) { Preconditions.checkArgument((maxSizeBytes) != (0)); } else { Preconditions.checkArgument(maxSizeBytes > 0); }
    this.maxSizeBytes = maxSizeBytes;
    this.messages = Lists.newLinkedList();
  }

  /**
   * Append the given message to this buffer, automatically evicting older messages until the
   * desired memory limit is achieved.
   */
  public synchronized void add(String messageText) {
    LogMessage message = new LogMessage(messageText, EnvironmentEdgeManager.currentTime());

    usage += message.estimateHeapUsage();
    messages.add(message);
    while (usage > maxSizeBytes) {
      LogMessage removed = messages.remove();
      usage -= removed.estimateHeapUsage();
      assert usage >= 0;
    }
  }

  /**
   * Dump the contents of the buffer to the given stream.
   */
  public synchronized void dumpTo(PrintWriter out) {
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    for (LogMessage msg : messages) {
      out.write(df.format(new Date(msg.timestamp)));
      out.write(" ");
      out.println(new String(msg.message, Charsets.UTF_8));
    }
  }

  synchronized List<LogMessage> getMessages() {
    // defensive copy
    return Lists.newArrayList(messages);
  }

  /**
   * Estimate the number of bytes this buffer is currently using.
   */
  synchronized long estimateHeapUsage() {
    return usage;
  }

  private static class LogMessage {
    /** the error text, encoded in bytes to save memory */
    public final byte[] message;
    public final long timestamp;

    /**
     * Completely non-scientific estimate of how much one of these objects takes, along with the
     * LinkedList overhead. This doesn't need to be exact, since we don't expect a ton of these
     * alerts.
     */
    private static final long BASE_USAGE = 100;

    public LogMessage(String message, long timestamp) {
      this.message = message.getBytes(Charsets.UTF_8);
      this.timestamp = timestamp;
    }

    public long estimateHeapUsage() {
      return message.length + BASE_USAGE;
    }
  }

}
