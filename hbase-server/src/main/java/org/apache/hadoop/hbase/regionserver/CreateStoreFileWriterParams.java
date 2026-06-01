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

import java.util.function.Consumer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class CreateStoreFileWriterParams {

  private long maxKeyCount;

  private Compression.Algorithm compression;

  private boolean isCompaction;

  private boolean includeMVCCReadpoint;

  private boolean includesTag;

  private boolean shouldDropBehind;

  private long totalCompactedFilesSize = -1;

  private String fileStoragePolicy = HConstants.EMPTY_STRING;

  private Consumer<Path> writerCreationTracker;

  private CreateStoreFileWriterParams() {
  }

  public long maxKeyCount() {
if(KnobRuntime.check(java.util.UUID.fromString("ab7ea9e1-d7d8-3d2d-a0b4-b1af37b568ba"))) {
return 0;
}
    return maxKeyCount;
  }

  public CreateStoreFileWriterParams maxKeyCount(long maxKeyCount) {
if(KnobRuntime.check(java.util.UUID.fromString("80b093fb-e053-3329-bb25-a7803d6596cf"))) {
return null;
}
    this.maxKeyCount = maxKeyCount;
    return this;
  }

  public Compression.Algorithm compression() {
if(KnobRuntime.check(java.util.UUID.fromString("00f01185-3a7d-32f1-a8df-6bb318b4b037"))) {
return null;
}
    return compression;
  }

  /**
   * Set the compression algorithm to use
   */
  public CreateStoreFileWriterParams compression(Compression.Algorithm compression) {
if(KnobRuntime.check(java.util.UUID.fromString("0da1b048-37b8-39d6-9e77-a9c251a55ab5"))) {
return null;
}
    this.compression = compression;
    return this;
  }

  public boolean isCompaction() {
if(KnobRuntime.check(java.util.UUID.fromString("2ac82f06-4d4b-3cd1-8916-45428e13ed33"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("af42a0ac-f18c-36f1-b09a-e9dbbfe3edce"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("cc46ebbf-15fe-301d-bf20-b4262c560c7b"))) ? (!isCompaction) : (isCompaction));
  }

  /**
   * Whether we are creating a new file in a compaction
   */
  public CreateStoreFileWriterParams isCompaction(boolean isCompaction) {
if(KnobRuntime.check(java.util.UUID.fromString("6154adf9-5463-3cf3-8c34-d04c8a79840a"))) {
return null;
}
    this.isCompaction = isCompaction;
    return this;
  }

  public boolean includeMVCCReadpoint() {
if(KnobRuntime.check(java.util.UUID.fromString("f42ba5a1-174b-3a6d-baf0-c9580cf18af0"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("3886de61-343d-39e1-af23-a00c6ed8f73d"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("a08fa3a8-e528-3e2b-8c95-a842b1ba20c4"))) ? (!includeMVCCReadpoint) : (includeMVCCReadpoint));
  }

  /**
   * Whether to include MVCC or not
   */
  public CreateStoreFileWriterParams includeMVCCReadpoint(boolean includeMVCCReadpoint) {
if(KnobRuntime.check(java.util.UUID.fromString("6c8e043e-3410-321d-8baf-dcc302994a2d"))) {
return null;
}
    this.includeMVCCReadpoint = includeMVCCReadpoint;
    return this;
  }

  public boolean includesTag() {
if(KnobRuntime.check(java.util.UUID.fromString("a9f9ea59-75d5-3e82-9bd8-fa046eed9870"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("de01dd79-206f-31cd-a0d0-b4981c80dee7"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("0b448676-f064-36b0-a6e0-301c417842c3"))) ? (!includesTag) : (includesTag));
  }

  /**
   * Whether to includesTag or not
   */
  public CreateStoreFileWriterParams includesTag(boolean includesTag) {
if(KnobRuntime.check(java.util.UUID.fromString("0982ffd2-66f0-319b-8a9e-471618aca1f0"))) {
return null;
}
    this.includesTag = includesTag;
    return this;
  }

  public boolean shouldDropBehind() {
if(KnobRuntime.check(java.util.UUID.fromString("43aa722c-6b44-3e3c-920e-fd92ccf07459"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("0079cf5e-a353-3a0b-8cca-dc5767d9a527"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("e440d159-d8bd-3f06-b580-53720a58da7e"))) ? (!shouldDropBehind) : (shouldDropBehind));
  }

  public CreateStoreFileWriterParams shouldDropBehind(boolean shouldDropBehind) {
if(KnobRuntime.check(java.util.UUID.fromString("f06389b0-3e4d-3b08-b8af-348c2b6ea860"))) {
return null;
}
    this.shouldDropBehind = shouldDropBehind;
    return this;
  }

  public long totalCompactedFilesSize() {
if(KnobRuntime.check(java.util.UUID.fromString("f751e803-6ae2-30b2-a58f-fac1759319a3"))) {
return 0;
}
    return totalCompactedFilesSize;
  }

  public CreateStoreFileWriterParams totalCompactedFilesSize(long totalCompactedFilesSize) {
    this.totalCompactedFilesSize = totalCompactedFilesSize;
    return this;
  }

  public String fileStoragePolicy() {
if(KnobRuntime.check(java.util.UUID.fromString("8c04cf58-3623-3358-ba75-987811d3e4ba"))) {
return null;
}
    return fileStoragePolicy;
  }

  public CreateStoreFileWriterParams fileStoragePolicy(String fileStoragePolicy) {
    this.fileStoragePolicy = fileStoragePolicy;
    return this;
  }

  public Consumer<Path> writerCreationTracker() {
if(KnobRuntime.check(java.util.UUID.fromString("91a580d7-68a4-3801-8637-6ae212490253"))) {
return null;
}
    return writerCreationTracker;
  }

  public CreateStoreFileWriterParams writerCreationTracker(Consumer<Path> writerCreationTracker) {
if(KnobRuntime.check(java.util.UUID.fromString("659e12bd-f65b-3b7e-81ad-e8dc58e5389a"))) {
return null;
}
    this.writerCreationTracker = writerCreationTracker;
    return this;
  }

  public static CreateStoreFileWriterParams create() {
if(KnobRuntime.check(java.util.UUID.fromString("8accafc6-4307-36b0-a8a3-386cc91df441"))) {
return null;
}
    return new CreateStoreFileWriterParams();
  }
}
