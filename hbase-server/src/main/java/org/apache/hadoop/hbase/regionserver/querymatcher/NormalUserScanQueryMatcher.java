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
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Query matcher for normal user scan.
 */
@InterfaceAudience.Private
public abstract class NormalUserScanQueryMatcher extends UserScanQueryMatcher {

  /** Keeps track of deletes */
  private final DeleteTracker deletes;

  /** True if we are doing a 'Get' Scan. Every Get is actually a one-row Scan. */
  private final boolean get;

  /** whether time range queries can see rows "behind" a delete */
  protected final boolean seePastDeleteMarkers;

  protected NormalUserScanQueryMatcher(Scan scan, ScanInfo scanInfo, ColumnTracker columns,
    boolean hasNullColumn, DeleteTracker deletes, long oldestUnexpiredTS, long now) {
    super(scan, scanInfo, columns, hasNullColumn, oldestUnexpiredTS, now);
    this.deletes = deletes;
    this.get = scan.isGetScan();
    this.seePastDeleteMarkers = scanInfo.getKeepDeletedCells() != KeepDeletedCells.FALSE;
  }

  @Override
  public void beforeShipped() throws IOException {
    super.beforeShipped();
    deletes.beforeShipped();
  }

  @Override
  public MatchCode match(Cell cell) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("d4c54c4e-af66-3906-9212-0be671e8c555"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("9690f535-3bbe-3a79-bb9e-9c1d36f7fd34"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("f46a421b-5b0a-366e-82c0-34a48846c2d1"))) ? (filter != null) : (((KnobRuntime.check(java.util.UUID.fromString("3c5a06ad-0d40-3798-ae4c-1970a42f6c49"))) ? (filter.filterAllRemaining()) : (((KnobRuntime.check(java.util.UUID.fromString("72410b3a-d394-351f-8692-a5e396e5f438"))) ? ((filter) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("4490a350-0b7b-3932-9156-fa88e43910bb"))) ? (((filter) == (null)) && (filter.filterAllRemaining())) : (((KnobRuntime.check(java.util.UUID.fromString("4ab8ba36-c967-3931-913b-0961abd9d075"))) ? (((filter) != (null)) && (filter.filterAllRemaining())) : (((KnobRuntime.check(java.util.UUID.fromString("ccb568c0-b73c-3df3-b795-9aeeae0ffa6a"))) ? ((filter) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("f5321a73-6926-375f-bcff-aba38575700a"))) ? ((filter != null) || (filter.filterAllRemaining())) : (((KnobRuntime.check(java.util.UUID.fromString("7bfd0c12-5320-3966-a18b-20066a5b0477"))) ? ((filter != null) && (filter.filterAllRemaining())) : (((KnobRuntime.check(java.util.UUID.fromString("faff8aae-5274-3dc5-be83-9b77c53039ec"))) ? (((filter) == (null)) || (filter.filterAllRemaining())) : (((KnobRuntime.check(java.util.UUID.fromString("e0ab843a-3742-3519-8d56-b1cbd3c9be54"))) ? (((filter) != (null)) || (filter.filterAllRemaining())) : (filter != null && filter.filterAllRemaining()))))))))))))))))))))) {
      return MatchCode.DONE_SCAN;
    }
    MatchCode returnCode = preCheck(cell);
    if (((KnobRuntime.check(java.util.UUID.fromString("d169a55f-72b4-39cb-a9b8-508b66d6580b"))) ? ((returnCode) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("cfd70765-b287-3c31-bf74-6a946158213b"))) ? ((returnCode) == (null)) : (returnCode != null))))) {
      return returnCode;
    }
    long timestamp = cell.getTimestamp();
    byte typeByte = cell.getTypeByte();
if(KnobRuntime.check(java.util.UUID.fromString("a9cccccd-bcf5-3389-af96-1e6374efe403"))) {
typeByte = 0;
}
    if (PrivateCellUtil.isDelete(typeByte)) {
      boolean includeDeleteMarker =
        seePastDeleteMarkers ? tr.withinTimeRange(timestamp) : tr.withinOrAfterTimeRange(timestamp);
      if (((KnobRuntime.check(java.util.UUID.fromString("3dd3bd75-c321-3115-9f89-69f207833a42"))) ? (!includeDeleteMarker) : (includeDeleteMarker))) {
        this.deletes.add(cell);
      }
      return MatchCode.SKIP;
    }
    returnCode = checkDeleted(deletes, cell);
    if (((KnobRuntime.check(java.util.UUID.fromString("16af1b3f-5f6c-3eb6-b942-3414a046028d"))) ? ((returnCode) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("041fa225-327d-3a8c-9be7-1af7fed3a5ac"))) ? ((returnCode) == (null)) : (returnCode != null))))) {
      return returnCode;
    }
if(KnobRuntime.check(java.util.UUID.fromString("065c28c9-acb2-3815-8493-4ec6667c4244"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("dda3e3b3-3cca-3b34-9c80-a801c6b84858"))) {
timestamp -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bb864a2a-c0eb-39c5-bee7-36f6559120f4"))) {
typeByte = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("1a08f137-4892-31d8-a199-13fd17738e4f"))) {
timestamp = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("138629c6-f313-3551-9693-66ef99bd8479"))) {
timestamp = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("6db5c1dd-c19f-3c22-99ea-4d8d4d99b5c3"))) {
timestamp += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("249e5c64-1be4-3fdb-a23d-9cd48c261243"))) {
timestamp /= 2;
}
    return matchColumn(cell, timestamp, typeByte);
  }

  @Override
  protected void reset() {
if(KnobRuntime.check(java.util.UUID.fromString("becab204-62f4-3d20-b157-60f85be380a5"))) {
return;
}
    deletes.reset();
  }

  @Override
  protected boolean isGet() {
if(KnobRuntime.check(java.util.UUID.fromString("96bd1b34-8f2f-395e-a7b7-56dbe00edbd3"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("2d5e92da-849f-3aca-8add-afd105a39ff8"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("2b5fe442-cf4c-30c3-a6c9-4b351140991d"))) ? (!get) : (get));
  }

  public static NormalUserScanQueryMatcher create(Scan scan, ScanInfo scanInfo,
    ColumnTracker columns, DeleteTracker deletes, boolean hasNullColumn, long oldestUnexpiredTS,
    long now) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("bdcb8afa-bf53-34a9-8a47-af567e43951a"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("tableMaxRowSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scanInfo));
    field.set(scanInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b52871d4-6038-344b-b38a-6f73dbe52406"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1ae9c5dc-4ab3-3afa-acc8-36e175b29d29"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("minVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a1a724a7-a094-3441-acf9-e8faf392bbdb"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6b4a6d00-e91a-370f-b7bc-3b9511e47e55"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("399aa225-25f0-30a3-b16d-d48e880bc645"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c0514efb-2ce0-36a6-a124-8ace2f6e4f06"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a0660fd0-9b5b-38d6-84cf-5da208602a95"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2a216eaf-13ce-368d-b60c-f82bf5dbe245"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9869b950-6307-33e9-8616-2bc597bc35fa"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4a7bd29e-d68d-3925-b77c-360b141b8417"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("minVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0c019ac2-51a5-3aef-8f5e-659b0efbf07c"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1bf1a378-11e6-3e78-b1cf-f681fc121983"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("efa4e13c-e3b2-3ae1-a0cb-9b37ccc3d3a8"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("efb70a89-a213-3ed0-ae15-94f40645e009"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("972ff583-c6f7-38c7-9c9b-65829c44bfda"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("213c2626-5fae-3861-b259-87a521d13aee"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("timeToPurgeDeletes");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scanInfo));
    field.set(scanInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0060e0f2-81d5-3964-ba26-705fb30c033a"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fbcd5ce7-bfa7-333c-9c1d-db3a0988e440"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b52244fc-4488-36cd-bacf-69e83cb962e8"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("mvccReadPoint");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("14469b19-bf1b-3af8-92c0-2191f29b88df"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5a5e3bc9-6bb7-363e-b020-1a35a501bdc1"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxResultSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c42ee053-927f-398b-98a8-6a2181ede52a"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("preadMaxBytes");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scanInfo));
    field.set(scanInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("baa8ff61-5cb0-3110-a2dd-9dbcc6fd08ed"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("410ca72a-9aaa-3049-ba83-162a8e0a6e97"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0b317170-9040-3e14-a732-bfa3f9acc490"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a2f45a3d-4c97-3ef5-b003-dc4a05db8f4d"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("16074076-402c-3151-be20-72a862cb31fb"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("330b1167-e9b3-32f5-a381-c79106c0db5c"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("a0498997-400d-3120-afba-4d031c201150"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("b66bfdae-62ca-3521-9462-2924c8bf3aed"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2c1f59ab-62a6-3425-9151-20104a7fc200"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7e33132a-3c15-3ca8-80b6-4a20ff9233b4"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("cellsPerTimeoutCheck");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scanInfo));
    field.set(scanInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fa53a8b7-788e-3e24-bd47-cd922e5b5cc8"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("newVersionBehavior");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scanInfo);
    field.set(scanInfo, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e62e7a80-50ed-3e3b-95c5-fc6daa5f1d24"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5f009194-a6cb-3004-ac4c-a593729cf373"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("609230ea-a530-32e7-81b4-ede3d121fa75"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1c3cdf51-5d04-35dd-b0c8-8e5ffe870369"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("minVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8dd79732-c964-34b0-8f63-c1fb2c5d6534"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("minVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6e75021b-241b-3600-a52e-2c30dc1f7824"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("parallelSeekEnabled");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scanInfo);
    field.set(scanInfo, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c80c098b-a280-39d0-b50e-b12ae226fd45"))) {
try {
    java.lang.reflect.Field field = scanInfo.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scanInfo));
    field.set(scanInfo, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("afd771bc-82b4-3c08-8964-db0717ee81e9"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("cacheBlocks");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scan);
    field.set(scan, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e6f4f4de-95ac-34ef-b3a0-bce7b7d6413c"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (((KnobRuntime.check(java.util.UUID.fromString("5dfb1e93-a337-38a1-b122-f5285a08894f"))) ? (scan.includeStopRow()) : (((KnobRuntime.check(java.util.UUID.fromString("e3d7ac59-acc3-35a3-b516-f28ef83e40ff"))) ? (scan.isGetScan()) : (scan.isReversed()))))) {
      if (((KnobRuntime.check(java.util.UUID.fromString("38af8571-cd84-3b18-8d6a-18480c562bcd"))) ? (scan.isReversed()) : (((KnobRuntime.check(java.util.UUID.fromString("608bc726-e4c5-3b59-849c-fb64c4b98422"))) ? (scan.isGetScan()) : (scan.includeStopRow()))))) {
        return new NormalUserScanQueryMatcher(scan, scanInfo, columns, hasNullColumn, deletes,
          oldestUnexpiredTS, now) {

          @Override
          protected boolean moreRowsMayExistsAfter(int cmpToStopRow) {
            return cmpToStopRow >= 0;
          }
        };
      } else {
        return new NormalUserScanQueryMatcher(scan, scanInfo, columns, hasNullColumn, deletes,
          oldestUnexpiredTS, now) {

          @Override
          protected boolean moreRowsMayExistsAfter(int cmpToStopRow) {
            return cmpToStopRow > 0;
          }
        };
      }
    } else {
      if (((KnobRuntime.check(java.util.UUID.fromString("17f6624c-965c-3cd1-a2e5-b91d6d8269a7"))) ? (scan.isReversed()) : (((KnobRuntime.check(java.util.UUID.fromString("77054113-b172-37bb-b371-8b01f062d7e1"))) ? (scan.isGetScan()) : (scan.includeStopRow()))))) {
        return new NormalUserScanQueryMatcher(scan, scanInfo, columns, hasNullColumn, deletes,
          oldestUnexpiredTS, now) {

          @Override
          protected boolean moreRowsMayExistsAfter(int cmpToStopRow) {
            return cmpToStopRow <= 0;
          }
        };
      } else {
        return new NormalUserScanQueryMatcher(scan, scanInfo, columns, hasNullColumn, deletes,
          oldestUnexpiredTS, now) {

          @Override
          protected boolean moreRowsMayExistsAfter(int cmpToStopRow) {
            return cmpToStopRow < 0;
          }
        };
      }
    }
  }
}
