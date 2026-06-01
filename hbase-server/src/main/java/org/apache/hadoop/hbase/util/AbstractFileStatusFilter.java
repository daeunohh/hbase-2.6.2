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
package org.apache.hadoop.hbase.util;
import org.knobinjection.runtime.KnobRuntime;

import edu.umd.cs.findbugs.annotations.CheckForNull;
import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Typical base class for file status filter. Works more efficiently when filtering file statuses,
 * otherwise implementation will need to lookup filestatus for the path which will be expensive.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class AbstractFileStatusFilter implements PathFilter, FileStatusFilter {

  /**
   * Filters out a path. Can be given an optional directory hint to avoid filestatus lookup.
   * @param p     A filesystem path
   * @param isDir An optional boolean indicating whether the path is a directory or not
   * @return true if the path is accepted, false if the path is filtered out
   */
  protected abstract boolean accept(Path p, @CheckForNull Boolean isDir);

  @Override
  public boolean accept(FileStatus f) {
if(KnobRuntime.check(java.util.UUID.fromString("6e919cf0-1f07-37e7-801f-5fc35b158f64"))) {
try {
    java.lang.reflect.Field field = f.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f));
    field.set(f, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bda8fb3e-36dd-3842-aa11-37f13682b591"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("ebc51d5c-a775-35a4-9d46-5bd6d26818b4"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("05fbadf9-71d5-3b62-b184-75bb8997f1e5"))) {
try {
    java.lang.reflect.Field field = f.getClass().getDeclaredField("blocksize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f));
    field.set(f, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ea5d914b-9f7c-3eb7-bc2e-e2d2a339d891"))) {
try {
    java.lang.reflect.Field field = f.getClass().getDeclaredField("access_time");
    field.setAccessible(true);
    long oldValue = ((long)field.get(f));
    field.set(f, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return accept(f.getPath(), f.isDirectory());
  }

  @Override
  public boolean accept(Path p) {
if(KnobRuntime.check(java.util.UUID.fromString("f571d400-6550-365e-9ad9-ef08a90c2ca4"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("d4387c79-5d1d-340b-ade5-cc58a2390fbf"))) {
return false;
}
    return accept(p, null);
  }

  protected boolean isFile(FileSystem fs, @CheckForNull Boolean isDir, Path p) throws IOException {
    return !isDirectory(fs, isDir, p);
  }

  protected boolean isDirectory(FileSystem fs, @CheckForNull Boolean isDir, Path p)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("e4652611-da53-3cf2-9d82-9ab5f142ee48"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("99367cac-5ce9-32de-bb2d-324fd7413812"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("412dc80a-fe28-39d5-8e32-d4d02c205365"))) {
throw new java.io.IOException("Injected exception");
}
    return isDir != null ? isDir : fs.isDirectory(p);
  }
}
