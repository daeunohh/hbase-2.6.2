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
if(KnobRuntime.check(java.util.UUID.fromString("e8bbc888-0efc-3ecb-a3ef-1f7f360eaf8f"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4d1f2601-e8c2-3b37-9721-7fc8b6d66013"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4a258cbd-f49d-3ec0-80ee-d572e34f1037"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("5de865bf-2d16-34fa-9976-5f4b446fa056"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fc11f109-c143-3c81-8bc2-dafc9a4847b0"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("id");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("336e415f-d3b1-33a5-888e-17d8c212cf94"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("15083cf4-b9b8-31a8-ac6d-0f757ce661b0"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("tagsPresent");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(snapshot);
    field.set(snapshot, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fe4ea925-1881-3ab5-8d5c-7f1a0b63307e"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    ArrayList<Path> result = new ArrayList<>();
    int cellsCount = snapshot.getCellsCount();
    if (((KnobRuntime.check(java.util.UUID.fromString("8cd7cc73-cb8e-3429-888f-6c11ba7b14a9"))) ? ((cellsCount) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("78520c2b-de9c-3c91-9317-72a0c396e477"))) ? ((cellsCount) != (0)) : (cellsCount == 0))))) return result; // don't flush if there are no entries

    // Use a store scanner to find which rows to flush.
if(KnobRuntime.check(java.util.UUID.fromString("993e8334-d400-37cc-a928-8e96c63658d5"))) {
throw new java.io.IOException("Injected exception");
}
    InternalScanner scanner = createScanner(snapshot.getScanners(), tracker);
    StoreFileWriter writer;
    try {
      // TODO: We can fail in the below block before we complete adding this flush to
      // list of store files. Add cleanup of anything put on filesystem if we fail.
      synchronized (flushLock) {
        if (KnobRuntime.check(java.util.UUID.fromString("6be8b3ca-a425-39d6-9d55-9a58f6a74bc9"))) { status.setStatus("Flushing "); } else if (KnobRuntime.check(java.util.UUID.fromString("6ca3fa25-c58f-3a72-a3c0-ede86feaf374"))) { status.setStatus("Flushing " + store); } else if (KnobRuntime.check(java.util.UUID.fromString("3d69760e-4b49-3d1d-88b2-44e208face3d"))) { status.setStatus((("Flushing ") + (store)) + (": creating writer")); } else if (KnobRuntime.check(java.util.UUID.fromString("327f0eab-2a9a-380c-9056-e925fee9a0f7"))) { status.setStatus(": creating writer"); } else if (KnobRuntime.check(java.util.UUID.fromString("1f94c8df-855f-351c-83a0-8749e22ed494"))) { status.setStatus(("Flushing ") + (": creating writer")); } else if (KnobRuntime.check(java.util.UUID.fromString("f4ce0775-694f-324a-bd5b-9ad556612764"))) { status.setStatus(("Flushing " + store) + (": creating writer")); } else if (KnobRuntime.check(java.util.UUID.fromString("c7e63e99-ecfd-3afe-b160-245d751b2e3b"))) { status.setStatus(("Flushing ") + (store)); } else { status.setStatus("Flushing " + store + ": creating writer"); }
        // Write the map out to the disk
if(KnobRuntime.check(java.util.UUID.fromString("44db95b7-3de6-3168-8534-7ede9c58bd82"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("tagsPresent");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(snapshot);
    field.set(snapshot, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("57648016-746d-3f80-9952-b93b7da29dbd"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("7f14c466-a97d-3711-8d45-4b6901fa0a1d"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("839af5f2-76f7-3ae7-9d6e-f2522e25a9a8"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3df592b0-8df8-39d2-9faa-61a5b01682f3"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("906d6002-4a99-3c92-a02f-b6f39d5123f2"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("id");
    field.setAccessible(true);
    long oldValue = ((long)field.get(snapshot));
    field.set(snapshot, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bffe4f82-bcbc-3f15-9ff6-ba6f0cf0ac6e"))) {
try {
    java.lang.reflect.Field field = snapshot.getClass().getDeclaredField("cellsCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(snapshot));
    field.set(snapshot, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        writer = createWriter(snapshot, false, writerCreationTracker);
        IOException e = null;
        try {
if(KnobRuntime.check(java.util.UUID.fromString("ee8587e5-a20d-3f94-8381-0e80694d49e1"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("livePutCellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(writer));
    field.set(writer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c5403831-c336-31d8-8b38-433444b561df"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("maxKeys");
    field.setAccessible(true);
    long oldValue = ((long)field.get(writer));
    field.set(writer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a86ace96-6a19-38b5-a186-c2162aee79f0"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("livePutCellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(writer));
    field.set(writer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("213daef5-44ec-361d-a80d-14f07b094298"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("livePutCellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(writer));
    field.set(writer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("04666cd4-ee74-3e84-9fe4-bc70aa5e7de1"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("maxKeys");
    field.setAccessible(true);
    long oldValue = ((long)field.get(writer));
    field.set(writer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3fa1ef58-ad01-3df9-99e0-ea70eb23d21b"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("newVersionBehavior");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(writer);
    field.set(writer, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e83bf688-95c1-3254-8f0d-e19a19b43646"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(writer));
    field.set(writer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("77b1a934-0e7b-37fd-b150-175e1afad7f8"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(writer));
    field.set(writer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2fe3bd9e-1402-36ca-88ae-0b9e2f5068ae"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("shouldDropCacheBehind");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(writer);
    field.set(writer, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("79fe251e-5003-3f03-99a6-5171d75dd1a2"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(writer));
    field.set(writer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c1efea76-e25b-32c4-806d-592ee008e199"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("25bd1378-69a0-3d87-9694-95086db9d3bc"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(writer));
    field.set(writer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9816fb64-45c5-3daa-9374-eefaa75ba6b4"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("livePutCellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(writer));
    field.set(writer, oldValue - 1);
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
          if (((KnobRuntime.check(java.util.UUID.fromString("418a60c4-3133-3364-ad7d-760d6f574f55"))) ? ((e) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("95fb556f-203a-3572-8d1a-12b0b6a43f81"))) ? ((e) == (null)) : (e != null))))) {
            writer.close();
          } else {
if(KnobRuntime.check(java.util.UUID.fromString("427ceecd-841c-3114-82a0-91876a1a9506"))) {
cacheFlushId = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("99f423e6-7aa1-3fb7-9097-061b6220200a"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(writer));
    field.set(writer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("35a369fa-2ef4-3b01-bc50-40974cef4a57"))) {
cacheFlushId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ac4a44e5-cbd3-3b28-a036-76537435e0e9"))) {
cacheFlushId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("4b075f9c-25b6-32b1-a63f-252ee488e8b7"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("maxKeys");
    field.setAccessible(true);
    long oldValue = ((long)field.get(writer));
    field.set(writer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bd0f93b2-f0e5-3508-887d-ea09d23f41c8"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("shouldDropCacheBehind");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(writer);
    field.set(writer, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("59f2e21f-1e95-38d8-8ef7-1a65e82e02f6"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("livePutCellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(writer));
    field.set(writer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("05992182-fa43-3921-aaa2-1aa3cfdea9e3"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("livePutCellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(writer));
    field.set(writer, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c07a8747-2ce3-3dfd-8c96-afc85a3179d1"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("66f0f692-fb08-383f-9651-5f9ed17df722"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("livePutCellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(writer));
    field.set(writer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("598014b0-5456-32eb-ad87-1404c871c0fe"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(writer));
    field.set(writer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("143f6ca1-0e99-3415-9dd1-bad5eb47cc61"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(writer));
    field.set(writer, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3212ff4d-f775-3d0b-bc8f-fbdcf682027d"))) {
cacheFlushId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("1f9f04c6-4739-3493-9178-dafaa86faff5"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("livePutCellCount");
    field.setAccessible(true);
    int oldValue = ((int)field.get(writer));
    field.set(writer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ff8fa131-a8aa-33d9-b6c4-262fb4b5abd2"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("maxKeys");
    field.setAccessible(true);
    long oldValue = ((long)field.get(writer));
    field.set(writer, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("332f11b7-dd94-3266-8d24-1c9921ddb832"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(writer));
    field.set(writer, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0785f229-d0d4-3236-bd92-45ea1eccfdff"))) {
try {
    java.lang.reflect.Field field = writer.getClass().getDeclaredField("newVersionBehavior");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(writer);
    field.set(writer, !oldValue);
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
if(KnobRuntime.check(java.util.UUID.fromString("f99b297c-c0ca-3cd5-9b55-465d98c9d262"))) {
throw new java.io.IOException("Injected exception");
}
      scanner.close();
    }
if(KnobRuntime.check(java.util.UUID.fromString("30082339-0f4f-355c-b9d4-e43f3f0a8b09"))) {
cacheFlushId += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d1db4f89-e5b9-3ce0-920a-c82dbf9b55f1"))) {
cacheFlushId -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("61fbd310-07f5-34be-bfec-637c059da3d7"))) {
cacheFlushId = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("2ff88828-866b-3bb9-84a5-4ccc6440d6c4"))) {
cacheFlushId = -1;
}
    LOG.info("Flushed memstore data size={} at sequenceid={} (bloomFilter={}), to={}",
      StringUtils.byteDesc(snapshot.getDataSize()), cacheFlushId, writer.hasGeneralBloom(),
      writer.getPath());
    result.add(writer.getPath());
    return result;
  }
}
