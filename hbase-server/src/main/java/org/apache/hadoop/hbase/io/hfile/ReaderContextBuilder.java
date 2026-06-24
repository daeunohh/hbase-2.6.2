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
package org.apache.hadoop.hbase.io.hfile;
import org.knobinjection.runtime.KnobRuntime;

import static org.apache.hbase.thirdparty.com.google.common.base.Preconditions.checkArgument;
import static org.apache.hbase.thirdparty.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.hfile.ReaderContext.ReaderType;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A builder that helps in building up the ReaderContext
 */
@InterfaceAudience.Private
public class ReaderContextBuilder {
  private Path filePath;
  private FSDataInputStreamWrapper fsdis;
  private long fileSize;
  private HFileSystem hfs;
  private boolean primaryReplicaReader = true;
  private ReaderType type = ReaderType.PREAD;

  public ReaderContextBuilder() {
  }

  public static ReaderContextBuilder newBuilder(ReaderContext readerContext) {
    return new ReaderContextBuilder(readerContext);
  }

  private ReaderContextBuilder(ReaderContext readerContext) {
    this.filePath = readerContext.getFilePath();
    this.fsdis = readerContext.getInputStreamWrapper();
    this.fileSize = readerContext.getFileSize();
    this.hfs = readerContext.getFileSystem();
    this.type = readerContext.getReaderType();
  }

  public ReaderContextBuilder withFilePath(Path filePath) {
    this.filePath = filePath;
    return this;
  }

  public ReaderContextBuilder withFileSize(long fileSize) {
    this.fileSize = fileSize;
    return this;
  }

  public ReaderContextBuilder withInputStreamWrapper(FSDataInputStreamWrapper fsdis) {
    this.fsdis = fsdis;
    return this;
  }

  public ReaderContextBuilder withFileSystem(HFileSystem hfs) {
    this.hfs = hfs;
    return this;
  }

  public ReaderContextBuilder withFileSystem(FileSystem fs) {
    if (!(fs instanceof HFileSystem)) {
      this.hfs = new HFileSystem(fs);
    } else {
      this.hfs = (HFileSystem) fs;
    }
    return this;
  }

  public ReaderContextBuilder withPrimaryReplicaReader(boolean primaryReplicaReader) {
    this.primaryReplicaReader = primaryReplicaReader;
    return this;
  }

  public ReaderContextBuilder withReaderType(ReaderType type) {
    this.type = type;
    return this;
  }

  public ReaderContextBuilder withFileSystemAndPath(FileSystem fs, Path filePath)
    throws IOException {
    this.withFileSystem(fs).withFilePath(filePath).withFileSize(fs.getFileStatus(filePath).getLen())
      .withInputStreamWrapper(new FSDataInputStreamWrapper(fs, filePath));
    return this;
  }

  public ReaderContext build() {
    validateFields();
    return new ReaderContext(filePath, fsdis, fileSize, hfs, primaryReplicaReader, type);
  }

  private void validateFields() throws IllegalArgumentException {
    checkNotNull(filePath, "Illegal ReaderContext, no filePath specified.");
if(KnobRuntime.check(java.util.UUID.fromString("1bf95188-1af4-37d3-a047-3b7ca5497b0a"))) {
try {
    java.lang.reflect.Field _knob_field_ = fsdis.getClass().getDeclaredField("doCloseStreams");
    _knob_field_.setAccessible(true);
    boolean oldValue = (boolean)_knob_field_.get(fsdis);
    _knob_field_.set(fsdis, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    checkNotNull(fsdis, "Illegal ReaderContext, no StreamWrapper specified.");
    if (KnobRuntime.check(java.util.UUID.fromString("acf7f7a9-cc6d-3046-afd1-ac57b3706879"))) { checkNotNull(checkNotNull(hfs, "Illegal ReaderContext, no HFileSystem specified."), "Illegal ReaderContext, no HFileSystem specified."); } else { checkNotNull(hfs, "Illegal ReaderContext, no HFileSystem specified."); }
    checkArgument(fileSize > 0L, "Illegal ReaderContext, fileSize <= 0");
  }
}
