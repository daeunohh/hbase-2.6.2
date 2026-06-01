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
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * ReversibleRegionScannerImpl extends from RegionScannerImpl, and is used to support reversed
 * scanning.
 */
@InterfaceAudience.Private
class ReversedRegionScannerImpl extends RegionScannerImpl {

  ReversedRegionScannerImpl(Scan scan, List<KeyValueScanner> additionalScanners, HRegion region,
    long nonceGroup, long nonce) throws IOException {
    super(scan, additionalScanners, region, nonceGroup, nonce);
  }

  @Override
  protected void initializeKVHeap(List<KeyValueScanner> scanners,
    List<KeyValueScanner> joinedScanners, HRegion region) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("3d777cbb-efb0-3d73-b244-21900992e045"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("memstoreFlushSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4ef6f90e-1d00-3dcd-8bfe-19c2fd02de1c"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("19321a03-771b-3771-af09-0619c1a305fe"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7b87a3e7-7336-38ee-9155-73259a55d9d3"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3b75fd2b-327f-30ba-af79-9cf4a7811bb3"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2c92b670-6457-3337-9bc9-82f071779ff9"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastFlushOpSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2bb620ef-87fe-3a33-9189-31508d3cb46b"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("439b3388-e851-37aa-a564-6ae387859c2d"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8c0ae94b-3239-3bb6-8efa-f7e858035ae4"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cb53ec34-54b3-3d9a-b2e9-e86259b6444f"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedCompactionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ba14fdf0-9251-3a91-9005-1e532fa2c820"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c7b90fd6-ef69-3919-ba19-e88a6b936646"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("aaece516-b8ac-3629-aaf3-fbb881e8531d"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("39142634-4d9b-39b4-b33d-d9dbd5c3a346"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4c2c1742-855d-3a61-b48d-7ab5af2c0eb6"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a25cd6c3-fc05-337d-9f11-a3c8b4a9bb3b"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("lastReplayedOpenRegionSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("29a34e03-7b0c-3922-a192-0db87f4667f6"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("blockingMemStoreSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("20b1a02a-4b20-3bdd-bf51-c20ae0119142"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("busyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c2cddcc8-a358-302f-9b9e-198c7ac84b77"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitDuration");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4c9f593b-2643-39bd-b0d9-b32b98984d5d"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b5dea80e-14d2-3845-a99e-b7d5de08b669"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("isRestoredRegion");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(region);
    field.set(region, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e7305467-5155-38a0-b431-caf1049d862c"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cfceec65-af6f-3d2b-8b6f-4aee867cb557"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("53e80042-6525-3bd5-850b-bfe777acb60d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("335f8e3e-fbc1-3050-ac82-db7af93cdae7"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("timestampSlop");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4ffe2769-adbb-3670-8e75-60ee24e7d7c9"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("openSeqNum");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("17f1cce1-fdb5-3f48-a72a-dc5dd822c87c"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2817de90-9b09-36db-842d-72d1b0a8a632"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("179aa508-decb-3617-8bd1-09218c4b99db"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8f61f7fa-04b3-3150-a49c-49a233988ca7"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowLockWaitDuration");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("65378987-10ca-35c5-9da6-a9e1ede1600a"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushPerChanges");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("29e8fdb8-e614-306e-b3dd-02962483fe8e"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxFlushedSeqId");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("57523c57-8528-3a0c-bd50-c3a7179c90f9"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("minBlockSizeBytes");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7ff99406-6de9-35fb-8b45-597f7909cc50"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("flushCheckInterval");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9c4a2cef-64c4-3198-b360-f17affe05a36"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxBusyWaitMultiplier");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("09e42860-c830-39ad-a08a-a460d02f09b1"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("rowProcessorTimeout");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7932eea5-c720-3888-bf8d-514744ddf4b3"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("13754459-80e3-33be-ab89-fc36e1473c19"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("maxCellSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(region));
    field.set(region, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("683ceb4b-26e8-3123-a365-129e1dd76ff1"))) {
try {
    java.lang.reflect.Field field = region.getClass().getDeclaredField("miniBatchSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(region));
    field.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    this.storeHeap = new ReversedKeyValueHeap(scanners, comparator);
    if (!joinedScanners.isEmpty()) {
      throw new DoNotRetryIOException("Reverse scan with loading CFs on demand is not supported");
    }
  }

  @Override
  protected boolean shouldStop(Cell currentRowCell) {
if(KnobRuntime.check(java.util.UUID.fromString("0591912a-97e2-303b-92bf-cf118732c42e"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("52cb72bc-5d4d-33d4-b1d4-c018a3eaa8a6"))) {
return true;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("e30b45a0-4909-315a-a660-94e8652050f3"))) ? ((currentRowCell) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("58ac0619-8477-36d9-ae2e-fa69a70de953"))) ? ((currentRowCell) != (null)) : (currentRowCell == null))))) {
      return true;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("8ba03a0e-30fb-3574-9769-7f4c07e56317"))) ? (Bytes.equals(stopRow, HConstants.EMPTY_START_ROW)) : (((KnobRuntime.check(java.util.UUID.fromString("10eacfc9-cfca-3852-a25e-2ed8f41b8cf4"))) ? (((stopRow) != (null)) || (Bytes.equals(stopRow, HConstants.EMPTY_START_ROW))) : (((KnobRuntime.check(java.util.UUID.fromString("b43b8174-4397-38e9-8a31-8387fd149429"))) ? (stopRow == null) : (((KnobRuntime.check(java.util.UUID.fromString("f06ee1ba-60e0-3cf6-9db2-6a9e68732cc4"))) ? ((stopRow) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d36c6709-81b0-3dc0-bafb-6a3183209e10"))) ? (((stopRow) == (null)) || (Bytes.equals(stopRow, HConstants.EMPTY_START_ROW))) : (((KnobRuntime.check(java.util.UUID.fromString("ce90c5c2-dbd4-3e30-9a35-cf6c5691400a"))) ? (((stopRow) == (null)) && (Bytes.equals(stopRow, HConstants.EMPTY_START_ROW))) : (((KnobRuntime.check(java.util.UUID.fromString("bf74280d-2d93-3f73-88f5-804652fd65fb"))) ? ((stopRow) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1ac6d7e5-6715-3f33-bef2-526c10e9bdbc"))) ? ((stopRow == null) && (Bytes.equals(stopRow, HConstants.EMPTY_START_ROW))) : (((KnobRuntime.check(java.util.UUID.fromString("3456d1e1-7a37-3951-919f-799cb2a8631b"))) ? ((stopRow == null) || (Bytes.equals(stopRow, HConstants.EMPTY_START_ROW))) : (((KnobRuntime.check(java.util.UUID.fromString("a1e8ed5f-515a-3854-be81-73fe41e6185f"))) ? (((stopRow) != (null)) && (Bytes.equals(stopRow, HConstants.EMPTY_START_ROW))) : (stopRow == null || Bytes.equals(stopRow, HConstants.EMPTY_START_ROW)))))))))))))))))))))) {
      return false;
    }
    int c = comparator.compareRows(currentRowCell, stopRow, 0, stopRow.length);
    return ((KnobRuntime.check(java.util.UUID.fromString("439de3d6-31ce-376c-abcd-0f18a39075cd"))) ? ((c) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ace672d8-76d5-305b-a10e-da12b9e09338"))) ? (((c) <= (0)) && ((c == 0 && !includeStopRow))) : (((KnobRuntime.check(java.util.UUID.fromString("f0ca7493-2ee4-331a-92ad-f48b2fe0dd52"))) ? (((c) > (0)) && ((c == 0 && !includeStopRow))) : (((KnobRuntime.check(java.util.UUID.fromString("ce8c4b32-6a10-3001-9b44-9be5d032a648"))) ? ((c < 0) || ((c == 0 && !includeStopRow))) : (((KnobRuntime.check(java.util.UUID.fromString("ea82ff05-7aea-384c-8e31-dcefaab3d260"))) ? (((c) == (0)) || ((c == 0 && !includeStopRow))) : (((KnobRuntime.check(java.util.UUID.fromString("0e1f12d9-9c90-3d8a-a802-2f828f8a1f63"))) ? (((c) >= (0)) && ((c == 0 && !includeStopRow))) : (((KnobRuntime.check(java.util.UUID.fromString("254f5ae0-f9a4-3f42-ad9b-025548c0219b"))) ? (((c) >= (0)) || ((c == 0 && !includeStopRow))) : (((KnobRuntime.check(java.util.UUID.fromString("dba5b3d7-e192-30ca-a32e-d77d6a26ab65"))) ? ((c < 0) && ((c == 0 && !includeStopRow))) : (((KnobRuntime.check(java.util.UUID.fromString("470fd802-f619-3910-8083-dc136b2d8ee5"))) ? (((c) == (0)) && ((c == 0 && !includeStopRow))) : (((KnobRuntime.check(java.util.UUID.fromString("0111f089-a5dc-3496-b0ca-d24cf69ebcff"))) ? (c < 0) : (((KnobRuntime.check(java.util.UUID.fromString("bb1a23e4-8e6d-39f8-b9e1-e3529efa7051"))) ? ((c) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("99d72c6c-a1fd-35ae-94be-138bf75bff3a"))) ? ((c) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("cffbb4c5-013a-330d-9767-ec14d1589b94"))) ? ((c) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1ba88fe2-2381-3880-985f-ce1e0c1c9bf3"))) ? ((c) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("be08220f-1ff1-34b1-9956-40e7bb34f30e"))) ? (((c) != (0)) || ((c == 0 && !includeStopRow))) : (((KnobRuntime.check(java.util.UUID.fromString("ed664e84-571f-3bcd-849c-65af3cf38c66"))) ? (((c) <= (0)) || ((c == 0 && !includeStopRow))) : (((KnobRuntime.check(java.util.UUID.fromString("5ebc39c9-bd40-3249-bfa0-db436584863b"))) ? ((c) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("92bb7e2c-b2b6-3562-ac4f-c367ff78c576"))) ? (((c) != (0)) && ((c == 0 && !includeStopRow))) : (((KnobRuntime.check(java.util.UUID.fromString("06e9922a-c2f9-3608-84bc-b54b18f49b67"))) ? ((c == 0 && !includeStopRow)) : (((KnobRuntime.check(java.util.UUID.fromString("5d1c4e36-836a-3c5e-a4f2-9b7414dd5be7"))) ? (((c) < (0)) && ((c == 0 && !includeStopRow))) : (((KnobRuntime.check(java.util.UUID.fromString("07a87d18-033d-3872-bd84-a563165fa86a"))) ? (((c) < (0)) || ((c == 0 && !includeStopRow))) : (((KnobRuntime.check(java.util.UUID.fromString("b53c7360-7fd4-31d3-86ff-f9e7254889c4"))) ? (((c) > (0)) || ((c == 0 && !includeStopRow))) : (c < 0 || (c == 0 && !includeStopRow)))))))))))))))))))))))))))))))))))))))))))));
  }

  @Override
  protected boolean nextRow(ScannerContext scannerContext, Cell curRowCell) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("cb2b6a72-be15-3560-9727-0a39758c73b7"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("keepProgress");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a3faddf7-49ce-39a7-8769-729aa95ba480"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("c09366b9-191b-34f5-ac5a-c5673c3bce0f"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("0b73eb88-5ab5-3a37-8351-566706acfb9a"))) {
try {
    java.lang.reflect.Field field = scannerContext.getClass().getDeclaredField("skippingRow");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scannerContext);
    field.set(scannerContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5a7f5951-dd1c-3592-afc0-568f080c3eec"))) {
return false;
}
    assert ((KnobRuntime.check(java.util.UUID.fromString("6bad2f43-7dba-33c0-8404-3e5c883f2f29"))) ? ((super.joinedContinuationRow) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("85469560-1c70-30b5-90e4-0954274347ed"))) ? ((super.joinedContinuationRow) == (null)) : (super.joinedContinuationRow == null)))) : "Trying to go to next row during joinedHeap read.";
if(KnobRuntime.check(java.util.UUID.fromString("f713853b-19ff-3e68-8832-196f15959d49"))) {
throw new java.io.IOException("Injected exception");
}
    this.storeHeap.seekToPreviousRow(PrivateCellUtil.createFirstOnRow(curRowCell));
if(KnobRuntime.check(java.util.UUID.fromString("88b1e3ce-aaeb-31e4-a9a8-bc2fff8a364e"))) {
throw new java.io.IOException("Injected exception");
}
    resetFilters();
    // Calling the hook in CP which allows it to do a fast forward
    if (((KnobRuntime.check(java.util.UUID.fromString("1e856532-d55a-34bd-bdd9-55714a293f82"))) ? ((this.region.getCoprocessorHost()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("7c306d30-dfd0-36a3-bc61-c91cc7e1d385"))) ? ((this.region.getCoprocessorHost()) != (null)) : (this.region.getCoprocessorHost() != null))))) {
if(KnobRuntime.check(java.util.UUID.fromString("097c650f-2acc-3f6e-acf7-8926ab0c9df9"))) {
throw new java.io.IOException("Injected exception");
}
      return this.region.getCoprocessorHost().postScannerFilterRow(this, curRowCell);
    }
    return true;
  }

}
