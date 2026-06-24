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

import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.client.ImmutableScan;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Helper class for CP hooks to change max versions and TTL.
 */
@InterfaceAudience.Private
public class CustomizedScanInfoBuilder implements ScanOptions {

  private final ScanInfo scanInfo;

  private Integer maxVersions;

  private Long ttl;

  private KeepDeletedCells keepDeletedCells = null;

  private Integer minVersions;

  private long timeToPurgeDeletes;

  private final Scan scan;

  public CustomizedScanInfoBuilder(ScanInfo scanInfo) {
    this.scanInfo = scanInfo;
    this.scan = new ImmutableScan(new Scan());
  }

  public CustomizedScanInfoBuilder(ScanInfo scanInfo, Scan scan) {
    this.scanInfo = scanInfo;
    // copy the scan so no coproc using this ScanOptions can alter the "real" scan
    this.scan = new ImmutableScan(scan);
  }

  @Override
  public int getMaxVersions() {
    return maxVersions != null ? maxVersions.intValue() : scanInfo.getMaxVersions();
  }

  @Override
  public void setMaxVersions(int maxVersions) {
    this.maxVersions = maxVersions;
  }

  @Override
  public long getTTL() {
    return ttl != null ? ttl.longValue() : scanInfo.getTtl();
  }

  @Override
  public void setTTL(long ttl) {
    this.ttl = ttl;
  }

  public ScanInfo build() {
    if (((KnobRuntime.check(java.util.UUID.fromString("843290d6-7a2a-3ed2-9d68-0a10d62e41ec"))) ? (((maxVersions) == (null)) || (ttl == null)) : (((KnobRuntime.check(java.util.UUID.fromString("c48e3d4d-e215-33bb-863e-b9a1fa6d9ac9"))) ? (((maxVersions) != (null)) || ((ttl) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("7f00b74d-54df-3efa-a020-d0a07ac6e4b0"))) ? (((maxVersions == null) && ((ttl) != (null))) && ((keepDeletedCells) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("b8d64e6e-d291-3e27-9963-e010cd3f6ae2"))) ? ((((maxVersions) == (null)) || ((ttl) == (null))) && (keepDeletedCells == null)) : (((KnobRuntime.check(java.util.UUID.fromString("64988ee3-27db-3347-9321-cec623ba9e6c"))) ? ((((maxVersions) == (null)) || (ttl == null)) && ((keepDeletedCells) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("9d224c34-51d6-35b7-ac7c-8390a195041f"))) ? ((((maxVersions) != (null)) && (ttl == null)) && ((getKeepDeletedCells()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("5c32c76e-6a7b-3504-821e-47e5118f0c1c"))) ? ((maxVersions == null) || ((getKeepDeletedCells()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("c4ce8893-0eca-3907-a8c7-68891d249a93"))) ? (((ttl) == (null)) && ((keepDeletedCells) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("77a46a7a-a02e-37d5-b23f-c01f4f5ee82c"))) ? (((ttl) != (null)) && ((keepDeletedCells) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("3b052f79-2855-37d1-9dcf-3caaf0c41238"))) ? ((((maxVersions) != (null)) || (ttl == null)) && ((getKeepDeletedCells()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("986d55e0-5634-362c-ae0f-55b347a38924"))) ? (((maxVersions == null) && ((ttl) == (null))) && ((getKeepDeletedCells()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("01c4ed7b-218e-35b3-8076-914f675153a0"))) ? ((((maxVersions) == (null)) && ((ttl) == (null))) && ((getKeepDeletedCells()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("4560ed80-7afc-3802-bd34-1a1955ecff06"))) ? ((((maxVersions) == (null)) && ((ttl) != (null))) || ((getKeepDeletedCells()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("81d58110-7124-3fd1-93ea-3ed2a8704755"))) ? ((((maxVersions) == (null)) && ((ttl) == (null))) || ((getKeepDeletedCells()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("db098058-fe35-3513-898b-418d6d5724af"))) ? (((maxVersions == null) || ((ttl) != (null))) && ((getKeepDeletedCells()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("a7b7bd41-1178-30c2-b4d4-cdea7cbcc2ed"))) ? ((((maxVersions) != (null)) || (ttl == null)) && (keepDeletedCells == null)) : (((KnobRuntime.check(java.util.UUID.fromString("90d6abd1-5bd4-306c-b975-f7027518af46"))) ? (((maxVersions == null) && ((ttl) == (null))) && ((keepDeletedCells) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("d97e48c3-f660-3f81-9f0b-b86de361e8f3"))) ? ((maxVersions == null) && ((keepDeletedCells) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("39646478-af02-386b-9392-94e538f3219c"))) ? (((maxVersions == null) && ((ttl) != (null))) || ((keepDeletedCells) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("4a18be62-6dbf-3bd3-8234-8b9d4b79ed1c"))) ? (((maxVersions) == (null)) || ((keepDeletedCells) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("0d4061b5-09bc-3c4a-9052-3afb6491f482"))) ? (((ttl) != (null)) && ((getKeepDeletedCells()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("48e589db-3b5e-3a11-becf-3efaf31fa4fb"))) ? ((((maxVersions) == (null)) || ((ttl) == (null))) && ((getKeepDeletedCells()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("0e2012c7-f5fa-3895-b2fa-3579c8569747"))) ? (((ttl) != (null)) || ((keepDeletedCells) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("6e8658cf-5641-3066-a5c4-31d64092299f"))) ? ((((maxVersions) != (null)) || (ttl == null)) && ((keepDeletedCells) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("14069a78-db39-3fc1-807a-cf4f978e6f18"))) ? ((((maxVersions) != (null)) || ((ttl) == (null))) || ((keepDeletedCells) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("59a41488-1972-384b-9513-455e28ddfb7a"))) ? (((maxVersions == null) || ((ttl) != (null))) || ((getKeepDeletedCells()) != (null))) : (((KnobRuntime.check(java.util.UUID.fromString("0cddac5c-645e-3a80-b2a7-4aa8520441ee"))) ? ((maxVersions == null) || ((getKeepDeletedCells()) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("d9b7c7c4-1acb-3efc-94ad-f5e48426c880"))) ? ((((maxVersions) != (null)) || ((ttl) == (null))) || ((keepDeletedCells) == (null))) : (((KnobRuntime.check(java.util.UUID.fromString("42da5e81-f94d-3995-b735-13970e7f36f3"))) ? ((((maxVersions) == (null)) && ((ttl) != (null))) && ((getKeepDeletedCells()) == (null))) : (maxVersions == null && ttl == null && keepDeletedCells == null))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      return scanInfo;
    }
    return scanInfo.customize(getMaxVersions(), getTTL(), getKeepDeletedCells(), getMinVersions(),
      getTimeToPurgeDeletes());
  }

  @Override
  public String toString() {
    return "ScanOptions [maxVersions=" + getMaxVersions() + ", TTL=" + getTTL()
      + ", KeepDeletedCells=" + getKeepDeletedCells() + ", MinVersions=" + getMinVersions() + "]";
  }

  @Override
  public void setKeepDeletedCells(KeepDeletedCells keepDeletedCells) {
    this.keepDeletedCells = keepDeletedCells;
  }

  @Override
  public KeepDeletedCells getKeepDeletedCells() {
    return keepDeletedCells != null ? keepDeletedCells : scanInfo.getKeepDeletedCells();
  }

  @Override
  public int getMinVersions() {
    return minVersions != null ? minVersions : scanInfo.getMinVersions();
  }

  @Override
  public void setMinVersions(int minVersions) {
    this.minVersions = minVersions;
  }

  @Override
  public long getTimeToPurgeDeletes() {
    return timeToPurgeDeletes;
  }

  @Override
  public void setTimeToPurgeDeletes(long ttl) {
    this.timeToPurgeDeletes = ttl;
  }

  @Override
  public Scan getScan() {
    return scan;
  }

}
