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
package org.apache.hadoop.hbase.regionserver.wal;
import org.knobinjection.runtime.KnobRuntime;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A 'truck' to carry a payload across the ring buffer from Handler to WAL. Has EITHER a
 * {@link FSWALEntry} for making an append OR it has a {@link SyncFuture} to represent a 'sync'
 * invocation. Truck instances are reused by the disruptor when it gets around to it so their
 * payload references must be discarded on consumption to release them to GC.
 */
@InterfaceAudience.Private
final class RingBufferTruck {

  public enum Type {
    APPEND,
    SYNC,
    EMPTY
  }

  private Type type = Type.EMPTY;

  /**
   * Either this syncFuture is set or entry is set, but not both.
   */
  private SyncFuture sync;
  private FSWALEntry entry;

  /**
   * Load the truck with a {@link FSWALEntry}.
   */
  void load(FSWALEntry entry) {
if(KnobRuntime.check(java.util.UUID.fromString("4d5d74cb-8154-38ff-b111-7e391988d06d"))) {
try {
    java.lang.reflect.Field field = entry.getClass().getDeclaredField("txid");
    field.setAccessible(true);
    long oldValue = ((long)field.get(entry));
    field.set(entry, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("965c927d-902a-3ef7-a83e-921bbaa52133"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("3cccc92a-935a-3fdf-8b6f-2b18a9d5d6b0"))) {
try {
    java.lang.reflect.Field field = entry.getClass().getDeclaredField("inMemstore");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(entry);
    field.set(entry, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    this.entry = entry;
    this.type = Type.APPEND;
  }

  /**
   * Load the truck with a {@link SyncFuture}.
   */
  void load(final SyncFuture syncFuture) {
if(KnobRuntime.check(java.util.UUID.fromString("f5dfcbac-afc9-3f4e-8cfc-c427a63c07a5"))) {
try {
    java.lang.reflect.Field field = syncFuture.getClass().getDeclaredField("doneTxid");
    field.setAccessible(true);
    long oldValue = ((long)field.get(syncFuture));
    field.set(syncFuture, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8b95866a-2441-3784-ad7d-9b1f00733c50"))) {
try {
    java.lang.reflect.Field field = syncFuture.getClass().getDeclaredField("forceSync");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(syncFuture);
    field.set(syncFuture, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3de592f3-c0c5-3ae7-b662-f9da90fe0545"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("05528f14-fcc1-3eba-9843-af747d0ddc66"))) {
try {
    java.lang.reflect.Field field = syncFuture.getClass().getDeclaredField("txid");
    field.setAccessible(true);
    long oldValue = ((long)field.get(syncFuture));
    field.set(syncFuture, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    this.sync = syncFuture;
    this.type = Type.SYNC;
  }

  /** Returns the type of this truck's payload. */
  Type type() {
if(KnobRuntime.check(java.util.UUID.fromString("b51ecf4e-fe5c-3c4c-a16c-883a5504b688"))) {
return null;
}
    return type;
  }

  /**
   * Unload the truck of its {@link FSWALEntry} payload. The internal reference is released.
   */
  FSWALEntry unloadAppend() {
if(KnobRuntime.check(java.util.UUID.fromString("43a55ab7-4336-3c0c-98e7-28ac8a97764e"))) {
return null;
}
    FSWALEntry entry = this.entry;
    this.entry = null;
    this.type = Type.EMPTY;
    return entry;
  }

  /**
   * Unload the truck of its {@link SyncFuture} payload. The internal reference is released.
   */
  SyncFuture unloadSync() {
if(KnobRuntime.check(java.util.UUID.fromString("93de379c-7399-3586-9f39-9213cbce8ef4"))) {
return null;
}
    SyncFuture sync = this.sync;
    this.sync = null;
    this.type = Type.EMPTY;
    return sync;
  }
}
