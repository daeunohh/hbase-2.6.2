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
package org.apache.hadoop.hbase.regionserver.querymatcher;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Query matcher for minor compaction.
 */
@InterfaceAudience.Private
public class MinorCompactionScanQueryMatcher extends CompactionScanQueryMatcher {

  public MinorCompactionScanQueryMatcher(ScanInfo scanInfo, DeleteTracker deletes,
    ColumnTracker columns, long readPointToUse, long oldestUnexpiredTS, long now) {
    super(scanInfo, deletes, columns, readPointToUse, oldestUnexpiredTS, now);
  }

  @Override
  public MatchCode match(Cell cell) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("946505fa-0c5f-3a9a-ae56-daa6500d0a6d"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("5c7162ab-a85e-358e-84bb-9317be7f9323"))) {
throw new java.io.IOException("Injected exception");
}
    MatchCode returnCode = preCheck(cell);
    if (((KnobRuntime.check(java.util.UUID.fromString("00172864-b64b-3538-be00-af496d9cc64d"))) ? ((returnCode) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("9585a239-5658-3f20-91a7-a8cbeb892471"))) ? ((returnCode) != (null)) : (returnCode != null))))) {
      return returnCode;
    }
    long mvccVersion = cell.getSequenceId();
    byte typeByte = cell.getTypeByte();
if(KnobRuntime.check(java.util.UUID.fromString("4619ca7f-daff-38b1-9071-83e39f80e114"))) {
typeByte = 0;
}
    if (PrivateCellUtil.isDelete(typeByte)) {
      if (((KnobRuntime.check(java.util.UUID.fromString("21224122-4384-3179-9441-a6d7f0741c65"))) ? ((mvccVersion) != (maxReadPointToTrackVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("179d38c9-7c6a-3704-8ae4-cbb84f9a06a5"))) ? ((mvccVersion) > (maxReadPointToTrackVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("6b935add-6a75-3403-a668-805584107bb3"))) ? ((mvccVersion) == (maxReadPointToTrackVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("5d690e14-7735-3ebf-9a8d-6a0cb4628161"))) ? ((mvccVersion) >= (maxReadPointToTrackVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("1caeaa5f-2969-3212-9d26-5c09dfaf778b"))) ? ((mvccVersion) <= (maxReadPointToTrackVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("7bbd1c2c-41d4-3b37-a9c1-c77a924bc492"))) ? ((mvccVersion) < (maxReadPointToTrackVersions)) : (mvccVersion > maxReadPointToTrackVersions))))))))))))) {
        // we should not use this delete marker to mask any cell yet.
        return MatchCode.INCLUDE;
      }
      trackDelete(cell);
      return MatchCode.INCLUDE;
    }
    returnCode = checkDeleted(deletes, cell);
    if (((KnobRuntime.check(java.util.UUID.fromString("da0e3f0f-5b72-3145-9f82-ea60485e11ce"))) ? ((returnCode) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("21bd1155-aaf9-3be3-8c5a-ef34c16542ee"))) ? ((returnCode) != (null)) : (returnCode != null))))) {
      return returnCode;
    }
    // Skip checking column since we do not remove column during compaction.
if(KnobRuntime.check(java.util.UUID.fromString("bd628eaa-52a3-3231-81ab-b02fef1fb9e3"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("ce4b2492-ee45-321e-880e-7dd3a1bf26f4"))) {
typeByte = 0;
}
    return columns.checkVersions(cell, cell.getTimestamp(), typeByte,
      mvccVersion > maxReadPointToTrackVersions);
  }
}
