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
    return accept(f.getPath(), f.isDirectory());
  }

  @Override
  public boolean accept(Path p) {
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
if(KnobRuntime.check(java.util.UUID.fromString("412dc80a-fe28-39d5-8e32-d4d02c205365"))) {
throw new java.io.IOException("Injected exception");
}
    return isDir != null ? isDir : fs.isDirectory(p);
  }
}
