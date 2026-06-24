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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of StoreFlusher.
 */
@InterfaceAudience.Private
public class DefaultStoreFlusher extends StoreFlusher {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultStoreFlusher.class);
  private final Object flushLock = new Object();

  public DefaultStoreFlusher(Configuration conf, HStore store) {
    super(conf, store);
  }

  @Override
  public List<Path> flushSnapshot(MemStoreSnapshot snapshot, long cacheFlushId,
    MonitoredTask status, ThroughputController throughputController, FlushLifeCycleTracker tracker,
    Consumer<Path> writerCreationTracker) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("39f0532c-71f3-3f60-89f0-f443d4ecefd8"))) {
try {
    java.lang.reflect.Field _knob_field_ = snapshot.getClass().getDeclaredField("cellsCount");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(snapshot));
    _knob_field_.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("336e415f-d3b1-33a5-888e-17d8c212cf94"))) {
throw new java.io.IOException("Injected exception");
}
    ArrayList<Path> result = new ArrayList<>();
    int cellsCount = snapshot.getCellsCount();
    if (((KnobRuntime.check(java.util.UUID.fromString("8cd7cc73-cb8e-3429-888f-6c11ba7b14a9"))) ? ((cellsCount) == (0)) : (cellsCount == 0))) return result; // don't flush if there are no entries

    // Use a store scanner to find which rows to flush.
    InternalScanner scanner = createScanner(snapshot.getScanners(), tracker);
    StoreFileWriter writer;
    try {
      // TODO: We can fail in the below block before we complete adding this flush to
      // list of store files. Add cleanup of anything put on filesystem if we fail.
      synchronized (flushLock) {
        status.setStatus("Flushing " + store + ": creating writer");
        // Write the map out to the disk
        writer = createWriter(snapshot, false, writerCreationTracker);
        IOException e = null;
        try {
if(KnobRuntime.check(java.util.UUID.fromString("afcdc9de-663d-3761-a852-789cc3ff1eb1"))) {
try {
    java.lang.reflect.Field _knob_field_ = writer.getClass().getDeclaredField("livePutCellCount");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(writer));
    _knob_field_.set(writer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("83f9efae-c610-3708-b929-761424b81e10"))) {
try {
    java.lang.reflect.Field _knob_field_ = writer.getClass().getDeclaredField("livePutCellCount");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(writer));
    _knob_field_.set(writer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fdbe201b-2c12-3f8b-b1e7-dc3a32689d52"))) {
try {
    java.lang.reflect.Field _knob_field_ = writer.getClass().getDeclaredField("maxKeys");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(writer));
    _knob_field_.set(writer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
          performFlush(scanner, writer, throughputController);
        } catch (IOException ioe) {
          e = ioe;
          // throw the exception out
          throw ioe;
        } finally {
          if (((KnobRuntime.check(java.util.UUID.fromString("95fb556f-203a-3572-8d1a-12b0b6a43f81"))) ? ((e) == (null)) : (e != null))) {
            writer.close();
          } else {
if(KnobRuntime.check(java.util.UUID.fromString("3b50ec29-c547-316a-9332-903f91d56606"))) {
try {
    java.lang.reflect.Field _knob_field_ = writer.getClass().getDeclaredField("maxVersions");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(writer));
    _knob_field_.set(writer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
            finalizeWriter(writer, cacheFlushId, status);
          }
        }
      }
    } finally {
      scanner.close();
    }
if(KnobRuntime.check(java.util.UUID.fromString("d1db4f89-e5b9-3ce0-920a-c82dbf9b55f1"))) {
cacheFlushId -= 1;
}
    LOG.info("Flushed memstore data size={} at sequenceid={} (bloomFilter={}), to={}",
      StringUtils.byteDesc(snapshot.getDataSize()), cacheFlushId, writer.hasGeneralBloom(),
      writer.getPath());
    result.add(writer.getPath());
    return result;
  }
}
