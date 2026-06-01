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
if(KnobRuntime.check(java.util.UUID.fromString("fce4a4c3-a97e-3eb5-b672-8b07d63fccc8"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("a62d1cc8-d86c-3887-86b8-d75117b8b32b"))) {
try {
    java.lang.reflect.Field field = readerContext.getClass().getDeclaredField("fileSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(readerContext));
    field.set(readerContext, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3a69aa0f-b896-318f-97f5-0c3c19931472"))) {
try {
    java.lang.reflect.Field field = readerContext.getClass().getDeclaredField("primaryReplicaReader");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(readerContext);
    field.set(readerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
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
if(KnobRuntime.check(java.util.UUID.fromString("dc6b8e11-b5cc-326e-8cbe-1dc7eaf51df5"))) {
return null;
}
    this.primaryReplicaReader = primaryReplicaReader;
    return this;
  }

  public ReaderContextBuilder withReaderType(ReaderType type) {
    this.type = type;
    return this;
  }

  public ReaderContextBuilder withFileSystemAndPath(FileSystem fs, Path filePath)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("9c950fdc-f459-365e-b2fa-91c0999671ee"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("9fe6d15f-3d9e-3566-b7e2-ab4e21ae6794"))) {
throw new java.io.IOException("Injected exception");
}
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
if(KnobRuntime.check(java.util.UUID.fromString("60c13203-a8ca-3101-b548-00e8ea40a399"))) {
try {
    java.lang.reflect.Field field = fsdis.getClass().getDeclaredField("useHBaseChecksumConfigured");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(fsdis);
    field.set(fsdis, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e0db3703-1bad-30b1-aeb4-c09dc03ba2e8"))) {
try {
    java.lang.reflect.Field field = fsdis.getClass().getDeclaredField("useHBaseChecksum");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(fsdis);
    field.set(fsdis, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("86aa1679-9753-32d8-bc33-99416e730208"))) {
try {
    java.lang.reflect.Field field = fsdis.getClass().getDeclaredField("doCloseStreams");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(fsdis);
    field.set(fsdis, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("41a42882-66f0-33de-b465-ef3634571b2d"))) {
try {
    java.lang.reflect.Field field = fsdis.getClass().getDeclaredField("readahead");
    field.setAccessible(true);
    long oldValue = ((long)field.get(fsdis));
    field.set(fsdis, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8eaad745-9dd3-3b01-b249-3cefd4093477"))) {
try {
    java.lang.reflect.Field field = fsdis.getClass().getDeclaredField("dropBehind");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(fsdis);
    field.set(fsdis, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (KnobRuntime.check(java.util.UUID.fromString("956bb045-c7f7-3917-af62-f106460830ed"))) { checkNotNull(checkNotNull(fsdis, "Illegal ReaderContext, no StreamWrapper specified."), "Illegal ReaderContext, no StreamWrapper specified."); } else { checkNotNull(fsdis, "Illegal ReaderContext, no StreamWrapper specified."); }
if(KnobRuntime.check(java.util.UUID.fromString("6d1589be-af32-3c08-947b-f6a7a41c9e9b"))) {
try {
    java.lang.reflect.Field field = hfs.getClass().getDeclaredField("useHBaseChecksum");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(hfs);
    field.set(hfs, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (KnobRuntime.check(java.util.UUID.fromString("acf7f7a9-cc6d-3046-afd1-ac57b3706879"))) { checkNotNull(checkNotNull(hfs, "Illegal ReaderContext, no HFileSystem specified."), "Illegal ReaderContext, no HFileSystem specified."); } else { checkNotNull(hfs, "Illegal ReaderContext, no HFileSystem specified."); }
    checkArgument(fileSize > 0L, "Illegal ReaderContext, fileSize <= 0");
  }
}
