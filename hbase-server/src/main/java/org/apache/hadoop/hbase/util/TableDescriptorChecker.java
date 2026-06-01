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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.fs.ErasureCodingUtils;
import org.apache.hadoop.hbase.regionserver.DefaultStoreEngine;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.ExploringCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.FIFOCompactionPolicy;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;

/**
 * Only used for master to sanity check {@link org.apache.hadoop.hbase.client.TableDescriptor}.
 */
@InterfaceAudience.Private
public final class TableDescriptorChecker {
  private static Logger LOG = LoggerFactory.getLogger(TableDescriptorChecker.class);

  public static final String TABLE_SANITY_CHECKS = "hbase.table.sanity.checks";
  public static final boolean DEFAULT_TABLE_SANITY_CHECKS = true;

  // should we check the compression codec type at master side, default true, HBASE-6370
  public static final String MASTER_CHECK_COMPRESSION = "hbase.master.check.compression";
  public static final boolean DEFAULT_MASTER_CHECK_COMPRESSION = true;

  // should we check encryption settings at master side, default true
  public static final String MASTER_CHECK_ENCRYPTION = "hbase.master.check.encryption";
  public static final boolean DEFAULT_MASTER_CHECK_ENCRYPTION = true;

  private TableDescriptorChecker() {
  }

  private static boolean shouldSanityCheck(final Configuration conf) {
    if (conf.getBoolean(TABLE_SANITY_CHECKS, DEFAULT_TABLE_SANITY_CHECKS)) {
      return true;
    }
    return false;
  }

  /**
   * Checks whether the table conforms to some sane limits, and configured values (compression, etc)
   * work. Throws an exception if something is wrong.
   */
  public static void sanityCheck(final Configuration c, final TableDescriptor td)
    throws IOException {
    CompoundConfiguration conf = new CompoundConfiguration().add(c).addBytesMap(td.getValues());

    // Setting logs to warning instead of throwing exception if sanityChecks are disabled
    boolean logWarn = !shouldSanityCheck(conf);

    // check max file size
    long maxFileSizeLowerLimit = 2 * 1024 * 1024L; // 2M is the default lower limit
    // if not set MAX_FILESIZE in TableDescriptor, and not set HREGION_MAX_FILESIZE in
    // hbase-site.xml, use maxFileSizeLowerLimit instead to skip this check
    long maxFileSize = td.getValue(TableDescriptorBuilder.MAX_FILESIZE) == null
      ? conf.getLong(HConstants.HREGION_MAX_FILESIZE, maxFileSizeLowerLimit)
      : Long.parseLong(td.getValue(TableDescriptorBuilder.MAX_FILESIZE));
if(KnobRuntime.check(java.util.UUID.fromString("12b1aed9-34df-34e8-a0e6-6bc118cd2214"))) {
maxFileSizeLowerLimit = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("34bf6f4b-2615-3911-8cdc-9e439e41d979"))) {
maxFileSizeLowerLimit *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("c6943437-e291-3098-b301-75268a8b9089"))) {
maxFileSizeLowerLimit -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1503c6ce-fc8d-3f3d-8b88-ad89651b60ec"))) {
maxFileSizeLowerLimit += 1;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("5b50bbc1-2daf-3db2-9640-0c59452f4e4d"))) ? ((2 * 1024 * 1024L) == (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("30a8fedb-ce60-37bb-94c2-a9c819746bf0"))) ? ((1024 * 1024L) < (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("943250dc-39f3-3a4c-9b3d-641da711756a"))) ? ((maxFileSize) <= (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("f5851b5c-a923-3476-b11c-795d4867cf26"))) ? ((maxFileSize) >= (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("1a215357-3be9-3101-95ec-d45a96d7713c"))) ? ((1024L) == (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("099aca75-bd26-3ad7-a3d9-60fdee8da02b"))) ? ((1024L) > (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("e9d32a75-fb1d-32ab-8b1e-4aba09caa9fa"))) ? ((2 * 1024 * 1024L) < (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("1dc9719a-d34e-3325-ad7a-56253ad990d3"))) ? ((maxFileSize) >= (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("b78b2310-0075-3ac5-9d85-09b58915347c"))) ? ((1024L) != (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("ade09132-1579-3ef6-9c6b-5951354524e3"))) ? ((2 * 1024 * 1024L) >= (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("7712e6cb-f576-3842-8a07-1aa867ed55f4"))) ? ((maxFileSize) == (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("8c7fb064-e627-393d-8e96-e779fe1f717b"))) ? ((1024L) == (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("28ef8951-dd59-3409-ba79-a47e5de0cd2b"))) ? ((2 * 1024 * 1024L) >= (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("c4b7a915-b585-3127-8c49-7066b3367e6c"))) ? ((maxFileSize) == (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("c7a54c87-024d-3757-9f92-414a14b6241f"))) ? ((1024L) <= (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("e99b113e-4117-395a-8e2c-2dd558ffe4b0"))) ? ((1024L) == (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("0a97d6af-7d02-390a-8a56-6857e513ca11"))) ? ((1024 * 1024L) <= (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("81c38300-8335-33e4-ab16-febfe9d1d275"))) ? ((maxFileSize) <= (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("b2bd4ba2-b9f4-3a18-8c5e-18aede294910"))) ? ((maxFileSize) != (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("8bb186e6-d70a-3ced-8e06-9f3e8e3df18d"))) ? ((1024L) <= (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("9e26aff5-8dc7-33b9-9986-9465c259e7fd"))) ? ((1024 * 1024L) <= (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("1aaadd19-7730-3197-ac1c-384eac5aae0a"))) ? ((2 * 1024 * 1024L) < (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("b294e70b-1f48-3958-affd-86437194a38c"))) ? ((1024L) != (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("13d3183a-f953-32b8-8e97-132c89cb0575"))) ? ((1024 * 1024L) >= (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("f104c33b-34e6-3f1e-b944-94d44c94831e"))) ? ((2 * 1024 * 1024L) != (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("a130ec90-c243-32f0-a236-638be02feb6f"))) ? ((1024 * 1024L) == (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("06dbfeda-436d-391f-9a71-c1911a4f624e"))) ? ((2 * 1024 * 1024L) >= (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("137bf3ba-d957-355a-b89c-a87876071674"))) ? ((1024 * 1024L) < (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("42e5c629-5502-3a32-bf99-b380150c060f"))) ? ((maxFileSize) <= (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("733870e2-7edf-3795-ba5c-62596a4c43cf"))) ? ((1024 * 1024L) != (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("2708bd90-c931-3e34-858b-f479a7290a8c"))) ? ((maxFileSize) < (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("23b8f555-6b74-3f00-912a-5115eabb52cf"))) ? ((maxFileSize) < (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("7bce9deb-ef9c-3efb-854b-8cfbea220edc"))) ? ((1024 * 1024L) > (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("66076b75-6ebe-3e20-9b29-74a22e780274"))) ? ((maxFileSize) > (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("cce806bc-7058-3b95-b3a8-c3a806406a90"))) ? ((maxFileSize) >= (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("ebd1de69-f754-341a-8dfe-c43b14fa66be"))) ? ((maxFileSize) < (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("d35d1fc7-8226-3121-a7b0-5dd61ebefe8a"))) ? ((1024 * 1024L) != (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("7db6c4de-4f2f-37c2-aae7-2766af30f001"))) ? ((2 * 1024 * 1024L) != (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("4d620a19-06ac-3591-b162-4372a366a6ef"))) ? ((1024 * 1024L) >= (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("e88b0afc-0174-3d41-a3d6-a3ea5a9b2927"))) ? ((1024 * 1024L) <= (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("59b7c086-76c3-3520-8a1d-10fce363d026"))) ? ((1024L) <= (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("a85b1dd3-189a-32f9-8e5f-996280371eed"))) ? ((2 * 1024 * 1024L) > (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("5d2f0843-38d6-3adf-941c-d2f3f2e63b6f"))) ? ((1024 * 1024L) != (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("9533c91b-919b-3b6f-8191-bea2530d2126"))) ? ((2 * 1024 * 1024L) != (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("97bab232-b218-31c7-ad54-fb3bbbd24801"))) ? ((1024 * 1024L) < (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("549f548c-d77d-38f3-aede-78cf9157201f"))) ? ((1024L) >= (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("1c050671-010d-32b0-9b1b-ec35f8720f5b"))) ? ((1024L) < (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("6ca8d6ca-912b-3194-bfba-dbf020cc083c"))) ? ((1024L) >= (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("5a4db33d-b02c-3fb8-9571-c8af69a63846"))) ? ((2 * 1024 * 1024L) > (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("9a402552-c1c0-3d65-94d8-7ce7075e1807"))) ? ((1024 * 1024L) > (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("71831478-bcfd-3ed3-aae3-4602f672925c"))) ? ((1024 * 1024L) == (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("60cc3e2d-b854-3880-93c8-dcb96349b6cf"))) ? ((maxFileSize) == (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("644ebf14-184f-3236-b918-030cb22bee1f"))) ? ((maxFileSize) > (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("234810a6-7e42-330c-8f5b-a560e55b52ed"))) ? ((2 * 1024 * 1024L) <= (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("9faf3881-6be1-36d2-a013-8f308c53837a"))) ? ((2 * 1024 * 1024L) < (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("e011d692-e4d3-3c5d-98a1-f7fa85faac6f"))) ? ((2 * 1024 * 1024L) <= (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("1fb39682-acce-3125-a0bc-6e5f289c693a"))) ? ((1024 * 1024L) > (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("cd254e8b-be62-30e4-8dc1-97b90090a356"))) ? ((2 * 1024 * 1024L) == (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("7853382a-30ab-3286-9012-2d4dee7c8e49"))) ? ((1024 * 1024L) >= (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("74b6b840-02bb-3ab5-a4ea-0c10956cabca"))) ? ((1024L) < (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("44c3f958-29a7-3f16-a5c1-68f349757b1d"))) ? ((1024 * 1024L) < (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("7906b0fc-4458-3722-9873-890ff410d172"))) ? ((2 * 1024 * 1024L) < (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("99b0761f-6a18-3f5e-989e-6ca49e8f3718"))) ? ((maxFileSize) != (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("9479e95f-e0ce-3603-b649-ea205ab006a5"))) ? ((2 * 1024 * 1024L) == (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("01a1b570-b14a-343d-987a-f912bb9ccb79"))) ? ((2 * 1024 * 1024L) <= (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("8a6de247-37b9-31bb-ae15-eb9b492f4f3e"))) ? ((1024 * 1024L) <= (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("50ec8723-c3ed-3664-9316-696aec586d2e"))) ? ((1024L) > (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("7aae0676-05b4-3516-aa1a-dec5ddbf7321"))) ? ((2 * 1024 * 1024L) > (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("ca27ffe4-8dd2-3f00-bfe0-2d335f548304"))) ? ((2 * 1024 * 1024L) <= (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("4871b225-c31b-39db-8604-6d559278d8d9"))) ? ((1024 * 1024L) == (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("36ba8a2c-9996-33ea-9358-e13411e49077"))) ? ((2 * 1024 * 1024L) == (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("a3ea6bcf-45e1-3dfd-aad1-b9e11b7caedd"))) ? ((maxFileSize) <= (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("0c23111b-47d4-3b41-b2ff-157befe06ab2"))) ? ((1024 * 1024L) >= (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("13793da5-e509-3db2-99b6-6dd593787b8a"))) ? ((maxFileSize) < (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("73472a3c-5022-3cb9-aa51-244462200af7"))) ? ((2 * 1024 * 1024L) != (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("9bfbe0a9-1a8e-3fde-862a-1a3d012b888d"))) ? ((1024L) <= (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("3f104efb-dc06-3f26-8d9e-d142d552f777"))) ? ((1024L) >= (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("95db83a4-6a60-3bb2-8527-70413bbc81ed"))) ? ((1024L) < (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("ccefd054-d2a2-39cc-b000-c30ca43ee662"))) ? ((1024L) < (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("6abb293a-de0e-30fb-9e2d-b874ba8a8fc4"))) ? ((1024L) != (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("946b300b-9116-3290-8dea-d4f57aedaf76"))) ? ((1024 * 1024L) == (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("56defa02-88cb-3a70-b4d6-56f6d47f6682"))) ? ((1024L) != (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("1551d697-e6fb-3e4b-a0a2-1c7b62ef2dc7"))) ? ((maxFileSize) >= (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("f30d7a10-85a8-33c9-a189-86d9212ab1e7"))) ? ((maxFileSize) != (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("b43a45c2-bbd4-3430-99c3-d6805d20df2b"))) ? ((maxFileSize) == (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("37f67ad0-47ce-33b0-8e94-a322c19ca020"))) ? ((1024L) == (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("2a1c34e3-7812-367f-b53b-e141952022c9"))) ? ((1024L) > (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("74e665e4-b9e1-3891-8a67-784ca05935f1"))) ? ((1024L) >= (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("c7b61777-2f85-379a-9da3-86774efab711"))) ? ((maxFileSize) > (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("bb3be98f-c025-323a-b98c-7ce6a69d2646"))) ? ((2 * 1024 * 1024L) > (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("b2ddbd3c-c2e8-3f03-99e5-983ec183450c"))) ? ((1024 * 1024L) != (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("ec2599e6-2f81-3811-87b6-ac3fa89ff42c"))) ? ((2 * 1024 * 1024L) >= (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("29b48a7a-db8f-3d43-b4de-34fc65f19c71"))) ? ((maxFileSize) > (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("d8755059-3768-3257-aa61-16392c116340"))) ? ((maxFileSize) != (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("51fc4cfc-e062-3425-8db6-a62861eaf6d7"))) ? ((1024 * 1024L) > (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("de410502-b3eb-32e1-8bbe-edf543e44114"))) ? ((1024L) > (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (maxFileSize < conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit)))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      String message = "MAX_FILESIZE for table descriptor or " + "\"hbase.hregion.max.filesize\" ("
        + maxFileSize + ") is too small, which might cause over splitting into unmanageable "
        + "number of regions.";
if(KnobRuntime.check(java.util.UUID.fromString("08037b04-96c4-3085-a4ea-d5386624e21d"))) {
logWarn = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("876ec411-990c-337a-99f8-f6405daf56e8"))) {
logWarn = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("a0c32bc6-33db-3820-a8db-1e33dbae2d05"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("62c5635c-53bf-3bf2-beb2-59232a9a2dc8"))) { warnOrThrowExceptionForFailure(!logWarn, message, null); } else if (KnobRuntime.check(java.util.UUID.fromString("74170a04-72db-356e-9180-4043d02ec053"))) { warnOrThrowExceptionForFailure(false, message, null); } else { warnOrThrowExceptionForFailure(logWarn, message, null); }
    }

    // check flush size
    long flushSizeLowerLimit = 1024 * 1024L; // 1M is the default lower limit
    // if not set MEMSTORE_FLUSHSIZE in TableDescriptor, and not set HREGION_MEMSTORE_FLUSH_SIZE in
    // hbase-site.xml, use flushSizeLowerLimit instead to skip this check
    long flushSize = td.getValue(TableDescriptorBuilder.MEMSTORE_FLUSHSIZE) == null
      ? conf.getLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, flushSizeLowerLimit)
      : Long.parseLong(td.getValue(TableDescriptorBuilder.MEMSTORE_FLUSHSIZE));
if(KnobRuntime.check(java.util.UUID.fromString("9ad50a0e-70ab-3043-8240-a81aaf5b55cc"))) {
flushSizeLowerLimit += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("95090468-173d-3f40-9171-628cccc9da9b"))) {
flushSizeLowerLimit = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8bf8473b-ec7e-3992-ae36-9ed5340aa662"))) {
flushSizeLowerLimit = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("37d4d01f-f493-31c2-a8e1-fd48e84a4954"))) {
flushSizeLowerLimit -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ea660e71-74ab-33de-891d-e35753eb9673"))) {
flushSizeLowerLimit *= 2;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("698ba514-505f-3169-b06b-9392d687a572"))) ? ((1024L) >= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("6214fa49-b095-3e95-8828-69bcee86a9bd"))) ? ((1024 * 1024L) == (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("28ff7cd5-0256-3a3a-a26e-4b5d4c0f4daa"))) ? ((2 * 1024 * 1024L) > (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("5122d935-7b23-3fa7-a34d-2ad228548e09"))) ? ((1024 * 1024L) >= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("44b63cc8-a0ea-3038-922f-d005f773b3e9"))) ? ((2 * 1024 * 1024L) >= (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("1c953b21-c12d-3a83-b696-1b927e302913"))) ? ((1024 * 1024L) > (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("151a2eb1-d643-32c2-9ec1-c9782c25cd6d"))) ? ((flushSize) < (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("0184ea85-8eb9-3109-9c5c-0b95a4d58d05"))) ? ((flushSize) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("a4800915-0f68-3b1b-b51f-fc0105e8fb8a"))) ? ((flushSize) != (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("0ae955b5-1a19-3ec9-9361-c2c53fd65084"))) ? ((1024L) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("0dd1a23f-5743-3ee6-a9c1-9bed8f302b5c"))) ? ((flushSize) >= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("542e07bd-84f1-377a-a48d-a99e968884af"))) ? ((flushSize) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("fb3452bc-a8da-3aaf-9973-252eca130b3a"))) ? ((1024L) < (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("17b6d6a0-613a-3e25-8b8e-ce4faccb3238"))) ? ((flushSize) < (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("047fcda1-ed96-3de8-a9b3-01da6ec679ea"))) ? ((1024L) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("7cffcca9-1f36-3ce0-bdb9-f0882602dbb3"))) ? ((2 * 1024 * 1024L) >= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("805000f4-1fdd-3481-b425-e14e78c16529"))) ? ((2 * 1024 * 1024L) > (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("f1423136-9088-314f-8bbc-f36a2aa9b8f5"))) ? ((2 * 1024 * 1024L) == (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("ac26273c-c853-3fb6-a7bc-9819e14d6564"))) ? ((1024L) == (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("26deb159-4b29-3c04-b398-4457b2e7f701"))) ? ((1024 * 1024L) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("98ce15ac-2076-3b13-abf0-950b475a0082"))) ? ((2 * 1024 * 1024L) > (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("415b59b8-7816-3367-8785-d74ab34c635a"))) ? ((1024 * 1024L) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("4844c959-6805-3fd4-89cf-88fe53a5f338"))) ? ((1024 * 1024L) < (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("45222529-c70e-3541-aee7-c4ad9e67779d"))) ? ((1024L) < (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("0550f693-f876-323d-9554-0f93f2a8cb9c"))) ? ((1024 * 1024L) >= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("d56c3abe-761a-3d7e-a8f1-0aa504db1975"))) ? ((1024L) != (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("97778517-6324-39ae-953c-b5afdcf21efc"))) ? ((2 * 1024 * 1024L) != (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("4912c023-11d4-3283-940c-187432285353"))) ? ((flushSize) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("b61d046f-500d-3947-9b8b-3db017ced8bd"))) ? ((1024 * 1024L) < (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("ce2717da-056e-379a-85cc-bc4db474907a"))) ? ((2 * 1024 * 1024L) == (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("5ba0bbf9-3df5-3ca5-aa4a-7b1bb6f8cc6f"))) ? ((2 * 1024 * 1024L) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("a70c1607-efb8-3dca-a42e-68d104dda66a"))) ? ((1024 * 1024L) > (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("91892289-2200-3ab6-80fe-d83568bc20d6"))) ? ((1024L) > (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("fc2f01b9-535c-3fd6-aa4c-eab555adcdc6"))) ? ((1024L) == (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("f1ab3c8c-c121-37ce-992f-9dc955e7c66d"))) ? ((1024 * 1024L) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("715d6313-803b-3752-9eb7-dbbffce17112"))) ? ((1024L) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("087bdbac-2474-323f-b6f4-3bdbbb210d43"))) ? ((2 * 1024 * 1024L) < (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("c279e1ea-0905-37ab-b720-1ef4b99d17b5"))) ? ((flushSize) >= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("0e951e51-b3b4-313d-ac8b-4439f2ba4abb"))) ? ((1024L) != (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("67540ecd-8008-3ec5-ba8d-d668328c92d7"))) ? ((flushSize) == (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("ce7e8e98-e00b-3971-bb0c-4ceff7ad59af"))) ? ((1024L) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("b293fe83-87ba-31e2-9628-73259f95edbb"))) ? ((flushSize) > (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("41c13aa5-d3a8-3708-91a8-5e8f64f5a67f"))) ? ((flushSize) == (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("e017da3e-855f-3824-abde-79fe736a1581"))) ? ((1024 * 1024L) != (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("2f195b02-3060-34e5-a7fe-9e3d0e3b7201"))) ? ((2 * 1024 * 1024L) >= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("c1be6c93-d111-3784-91b9-3df80771e200"))) ? ((flushSize) != (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("d3e16f37-db0c-37a1-849c-6cb3e808c6b0"))) ? ((flushSize) > (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("d99889d6-ddc3-3f33-8a3e-1ba9e5437379"))) ? ((1024 * 1024L) > (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("df379462-7987-3e2d-8ed1-9c3162421c4e"))) ? ((flushSize) >= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("15b90bbf-0eac-3d1e-b856-919a40e954cb"))) ? ((2 * 1024 * 1024L) == (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("a8ae63b4-4f67-3dc3-9e0d-8ec27cd7f60d"))) ? ((2 * 1024 * 1024L) >= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("1955e7e8-90c4-3083-b9bb-8230064db6b3"))) ? ((2 * 1024 * 1024L) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("150d8a36-9d41-3786-b6fa-bd2365513258"))) ? ((2 * 1024 * 1024L) != (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("9d006f06-a6a6-3d1e-b1bd-e26630093455"))) ? ((2 * 1024 * 1024L) < (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("93b43d0a-0838-3b3d-83c1-7b939037a0ad"))) ? ((2 * 1024 * 1024L) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("ed7df8f1-ff62-36fc-8f0b-1fae57057167"))) ? ((1024L) == (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("9b104717-1e25-36f7-8470-a9dc2b496ee9"))) ? ((flushSize) != (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("6c0a7ebc-50bf-3a68-a608-c86422639876"))) ? ((1024L) >= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("d0de8443-f216-36b0-81bb-955f56782781"))) ? ((1024L) >= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("f457f6ef-91f8-3ba1-9052-d9d0b6b50d39"))) ? ((2 * 1024 * 1024L) < (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("8b41b019-395f-3801-bb26-8a3fe3014515"))) ? ((2 * 1024 * 1024L) > (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("dca362e7-1995-3cf8-ac7b-cc7e5540805d"))) ? ((1024 * 1024L) == (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("f5903692-4126-38d5-bc5d-4f89bd225efb"))) ? ((1024 * 1024L) != (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("b1074458-07e8-3d9e-81e7-fa155d4d46e2"))) ? ((2 * 1024 * 1024L) < (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("5062f47d-8b6e-34f9-9e4e-3b16af24920f"))) ? ((1024 * 1024L) != (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("60eb1ff7-d165-3e94-853b-91401961adc6"))) ? ((1024 * 1024L) < (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("1422a3dc-3555-3408-908e-c376a18e77f8"))) ? ((flushSize) != (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("28e6a6fb-3da6-3f55-87d2-4ac19ebb8510"))) ? ((1024 * 1024L) < (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("875a80a8-c81f-3d76-a413-9e182ccc52b8"))) ? ((flushSize) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("263df3ae-b9c6-35b9-8515-22d71cb1c153"))) ? ((2 * 1024 * 1024L) == (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("e9c445c6-4ee2-36f3-aadd-086de24da528"))) ? ((flushSize) == (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("56f9c1f4-0671-34e7-900a-64391850b330"))) ? ((2 * 1024 * 1024L) != (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("2048214b-4b02-34b1-b0ae-9978119011e7"))) ? ((flushSize) > (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("e0608f7e-eb24-3a27-abe3-a99b7516606e"))) ? ((flushSize) < (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("86022a2c-746a-3ae3-ac55-5c5d5d2813ef"))) ? ((1024L) > (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("0a4d6315-bfbd-311c-8f84-3ac99f4c126a"))) ? ((1024 * 1024L) >= (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("1540a555-2988-3e87-bcf5-656792d138c3"))) ? ((1024 * 1024L) == (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("adecc0d6-c97e-3622-ab5f-954f87092ab6"))) ? ((1024L) > (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("4e808db3-b3c2-3dfd-9ca9-d9aae14777c4"))) ? ((1024L) < (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("0e0a9aee-8e36-3fd5-935e-359196dcbdb7"))) ? ((1024 * 1024L) != (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("032d35da-76a3-30e3-b504-9749ee073e70"))) ? ((1024 * 1024L) > (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("84e3ca6d-c3eb-34a0-a925-9a7ad7cd217e"))) ? ((2 * 1024 * 1024L) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("a2889892-c16b-3018-b753-a0296502ea9f"))) ? ((flushSize) > (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("e18e2878-48c4-3f4f-a7ec-1b21a17f9b30"))) ? ((1024 * 1024L) >= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("f5117a87-3f64-3a42-9192-d37bade47b11"))) ? ((1024L) == (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("74498457-c4ec-3dc8-8388-3b400eec3298"))) ? ((1024L) >= (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("baf874aa-69d3-33a6-b4f7-e44dba8834b1"))) ? ((flushSize) < (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("80fd7ade-5f70-3da4-9ff2-b38755ab1e77"))) ? ((1024L) < (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("c438e503-ca20-3781-9793-1be6505224f3"))) ? ((1024 * 1024L) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("d44f9a1d-2895-33d2-ba1f-f90ce8d8b20f"))) ? ((1024L) != (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("47b32ece-92e7-3568-82da-13b8c054dae8"))) ? ((1024L) != (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("5544ff22-513b-3b1b-ad2c-08a4b3691875"))) ? ((1024L) > (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("e64c2822-29ac-3de0-a80a-570570458c1e"))) ? ((2 * 1024 * 1024L) != (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("f1c2e7aa-fd98-3f67-b270-a687e9f0fc44"))) ? ((flushSize) == (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("69d6f70d-9f3b-3d5b-95bc-d4149d9cfb73"))) ? ((flushSize) >= (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("a81311e2-200b-38ee-8392-12199acaf3ea"))) ? ((1024 * 1024L) == (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (flushSize < conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit)))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      String message =
        "MEMSTORE_FLUSHSIZE for table descriptor or " + "\"hbase.hregion.memstore.flush.size\" ("
          + flushSize + ") is too small, which might cause" + " very frequent flushing.";
      warnOrThrowExceptionForFailure(logWarn, message, null);
    }

    // check that coprocessors and other specified plugin classes can be loaded
    checkClassLoading(conf, td);

    if (conf.getBoolean(MASTER_CHECK_COMPRESSION, DEFAULT_MASTER_CHECK_COMPRESSION)) {
      // check compression can be loaded
if(KnobRuntime.check(java.util.UUID.fromString("2d30c8ce-61df-3f1a-a2da-77b0b1b18087"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("baae628b-e2b0-3a6f-9fa6-0e7405b25961"))) { checkEncryption(new CompoundConfiguration(), td); } else if (KnobRuntime.check(java.util.UUID.fromString("10923777-abb5-355c-9084-7bcff75e7379"))) { checkClassLoading(new CompoundConfiguration(), td); } else if (KnobRuntime.check(java.util.UUID.fromString("250580e9-1dfa-372f-8188-008e07262d31"))) { checkCompression(new CompoundConfiguration(), td); } else { checkCompression(conf, td); }
    }

    if (((KnobRuntime.check(java.util.UUID.fromString("2ff44841-98d7-35af-8904-8b231f290fb2"))) ? (conf.getBoolean(MASTER_CHECK_ENCRYPTION, !DEFAULT_MASTER_CHECK_ENCRYPTION)) : (((KnobRuntime.check(java.util.UUID.fromString("91e37a39-2abd-35c5-95c6-3fd77bf1875d"))) ? (conf.getBoolean(MASTER_CHECK_ENCRYPTION, false)) : (conf.getBoolean(MASTER_CHECK_ENCRYPTION, DEFAULT_MASTER_CHECK_ENCRYPTION)))))) {
      // check encryption can be loaded
      checkEncryption(conf, td);
    }

    // Verify compaction policy
    checkCompactionPolicy(conf, td);
    // check that we have at least 1 CF
    if (((KnobRuntime.check(java.util.UUID.fromString("f56302ea-0771-368e-afb7-bc18b0845a6b"))) ? ((td.getRegionReplication()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6c1c70eb-e859-3f66-88aa-23ccad97fdd8"))) ? ((td.getColumnFamilyCount()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("95ba7644-cd35-31ba-a125-13377254f7a1"))) ? ((td.getColumnFamilyCount()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6e4dba19-18a6-3a67-ad20-66f89eac587d"))) ? ((td.getRegionReplication()) != (0)) : (td.getColumnFamilyCount() == 0))))))))) {
      String message = "Table should have at least one column family.";
if(KnobRuntime.check(java.util.UUID.fromString("04a0caff-0e5d-3f1f-b90b-dbc088ee8b3f"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("4ec57126-4446-370c-8597-c3c41c266249"))) {
logWarn = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("f462b503-6169-3d37-86ae-968a6894665d"))) {
logWarn = true;
}
      if (KnobRuntime.check(java.util.UUID.fromString("494c4f90-750a-3cd3-81c3-172a12fcf861"))) { warnOrThrowExceptionForFailure(!logWarn, message, null); } else if (KnobRuntime.check(java.util.UUID.fromString("36ed32e5-27de-3bdf-984f-3e5bc26080ae"))) { warnOrThrowExceptionForFailure(false, message, null); } else { warnOrThrowExceptionForFailure(logWarn, message, null); }
    }

    // check that we have minimum 1 region replicas
    int regionReplicas = td.getRegionReplication();
    if (((KnobRuntime.check(java.util.UUID.fromString("20a6fb36-086a-318a-a658-ed058626e2dc"))) ? ((16 * 1024 * 1024) > (1)) : (((KnobRuntime.check(java.util.UUID.fromString("9f1b7983-024c-3c14-b1f6-6bbee8e78610"))) ? ((16 * 1024) >= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("9120ce23-26b3-3ae8-8e55-9dad99c117b1"))) ? ((regionReplicas) < (1)) : (((KnobRuntime.check(java.util.UUID.fromString("e151d9b5-151d-352b-969d-a04414adfae7"))) ? ((2 * 1024) >= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("1872d8cd-ac2d-3cdb-bd04-3222594ff8d4"))) ? ((regionReplicas) <= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("ad4f713a-8888-382d-8850-71f11fcd3128"))) ? ((2 * 1024) <= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("08f26458-94eb-3511-a1ad-3da08df935c3"))) ? ((2 * 1024) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("25af6da8-38c7-388a-9b63-91aa5b809ecf"))) ? ((16 * 1024 * 1024) < (1)) : (((KnobRuntime.check(java.util.UUID.fromString("19031245-2c8f-375d-bdc9-da4a70dbdd36"))) ? ((16 * 1024) <= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("96e174e0-3a08-3cba-bd3e-dd10c8a42328"))) ? ((16 * 1024 * 1024) >= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("f8f515a0-df12-3bd6-ae0e-13294620fce6"))) ? ((16 * 1024) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("24bf828c-7a60-326d-a4c4-fce81cc8bd5c"))) ? ((16 * 1024) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("aa02f893-0606-390c-9a8b-c98d95ca5f55"))) ? ((2 * 1024) < (1)) : (((KnobRuntime.check(java.util.UUID.fromString("ec72ed09-6679-3769-9d7d-c17ddcf09155"))) ? ((regionReplicas) >= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("2e16d591-1cfc-39db-9c95-1a189e10b939"))) ? ((regionReplicas) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("05642f24-65dd-3f00-87ae-96c4c379d05d"))) ? ((16 * 1024) > (1)) : (((KnobRuntime.check(java.util.UUID.fromString("2e9825e4-1acf-321e-bb4a-317bea7b4c2f"))) ? ((2 * 1024) > (1)) : (((KnobRuntime.check(java.util.UUID.fromString("b07f2e81-2cc7-3730-8a37-2052c56ac79b"))) ? ((regionReplicas) > (1)) : (((KnobRuntime.check(java.util.UUID.fromString("28743731-b359-3a01-9b82-d98c439fdf9c"))) ? ((2 * 1024) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("8520f381-441a-3025-b343-f4b14e314470"))) ? ((16 * 1024) < (1)) : (((KnobRuntime.check(java.util.UUID.fromString("cfe94519-03df-3011-9abc-6f93eee4c631"))) ? ((regionReplicas) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("d75629c2-44f3-39cb-a90b-58cc9e44c8f4"))) ? ((16 * 1024 * 1024) <= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("3896b5f0-0c35-3988-b908-3c2b60ecddbd"))) ? ((16 * 1024 * 1024) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("77206557-f5b2-39f5-acc0-93ef1a4ca87d"))) ? ((16 * 1024 * 1024) == (1)) : (regionReplicas < 1))))))))))))))))))))))))))))))))))))))))))))))))) {
      String message = "Table region replication should be at least one.";
if(KnobRuntime.check(java.util.UUID.fromString("3fb79257-7076-3141-8d11-c6d76d357f93"))) {
logWarn = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("31ce45a0-2ee7-3510-b325-8e4866650ead"))) {
logWarn = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("9893a987-1ec8-325d-9985-8fe3078a1994"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("368a6dec-3304-308d-af2a-3bf4b84e9bfa"))) { warnOrThrowExceptionForFailure(!logWarn, message, null); } else if (KnobRuntime.check(java.util.UUID.fromString("b1b3fbf2-b963-3f27-9a2c-342514ce247d"))) { warnOrThrowExceptionForFailure(false, message, null); } else { warnOrThrowExceptionForFailure(logWarn, message, null); }
    }

    // Meta table shouldn't be set as read only, otherwise it will impact region assignments
    if (((KnobRuntime.check(java.util.UUID.fromString("65624f0f-b3b6-332a-b817-9dbd02566134"))) ? ((td.isReadOnly()) && (TableName.isMetaTableName(td.getTableName()))) : (((KnobRuntime.check(java.util.UUID.fromString("9bd8a095-a9aa-3a58-89ad-70bd98fa6405"))) ? (td.isReadOnly()) : (((KnobRuntime.check(java.util.UUID.fromString("b2caefeb-8ffd-3020-886b-3ec69c53cf90"))) ? ((td.isReadOnly()) || (TableName.isMetaTableName(td.getTableName()))) : (((KnobRuntime.check(java.util.UUID.fromString("b85259f0-3d21-3bfe-acb7-1250c8b43e94"))) ? (TableName.isMetaTableName(td.getTableName())) : (td.isReadOnly() && TableName.isMetaTableName(td.getTableName())))))))))) {
      warnOrThrowExceptionForFailure(false, "Meta table can't be set as read only.", null);
    }

    // check replication scope
    checkReplicationScope(conf, td);

    // check bloom filter type
if(KnobRuntime.check(java.util.UUID.fromString("e0b0d044-e557-3336-8f7b-8e4b37567352"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("dcfc319b-1cce-32f9-b0b1-e23b8cfa8501"))) { checkEncryption(new CompoundConfiguration(), td); } else if (KnobRuntime.check(java.util.UUID.fromString("995f7d51-f883-37b9-8418-9b259d57a450"))) { checkCompression(new CompoundConfiguration(), td); } else if (KnobRuntime.check(java.util.UUID.fromString("ace8ec04-bb7f-373f-b4f8-9aa3245b5df7"))) { checkClassLoading(new CompoundConfiguration(), td); } else { checkBloomFilterType(conf, td); }

    if (td.getErasureCodingPolicy() != null) {
      warnOrThrowExceptionForFailure(logWarn,
        () -> ErasureCodingUtils.verifySupport(conf, td.getErasureCodingPolicy()));
    }

    for (ColumnFamilyDescriptor hcd : td.getColumnFamilies()) {
      if (hcd.getTimeToLive() <= 0) {
        String message = "TTL for column family " + hcd.getNameAsString() + " must be positive.";
        warnOrThrowExceptionForFailure(logWarn, message, null);
      }

      // check blockSize
      if (hcd.getBlocksize() < 1024 || hcd.getBlocksize() > 16 * 1024 * 1024) {
        String message = "Block size for column family " + hcd.getNameAsString()
          + "  must be between 1K and 16MB.";
if(KnobRuntime.check(java.util.UUID.fromString("24b0d500-5f6b-3879-b023-859f84739b86"))) {
logWarn = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("82c88488-5de6-31ba-a433-065dbedffdf6"))) {
logWarn = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("c67f93e2-8503-3e2c-bea9-e173e625c5dc"))) {
throw new java.io.IOException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("852b8831-e134-3178-ad49-93f5f85e2bf4"))) { warnOrThrowExceptionForFailure(false, message, null); } else if (KnobRuntime.check(java.util.UUID.fromString("6e333be0-b023-3282-a347-cf2f32c5ebcf"))) { warnOrThrowExceptionForFailure(!logWarn, message, null); } else { warnOrThrowExceptionForFailure(logWarn, message, null); }
      }

      // check versions
      if (hcd.getMinVersions() < 0) {
        String message =
          "Min versions for column family " + hcd.getNameAsString() + "  must be positive.";
        warnOrThrowExceptionForFailure(logWarn, message, null);
      }
      // max versions already being checked

      // HBASE-13776 Setting illegal versions for ColumnFamilyDescriptor
      // does not throw IllegalArgumentException
      // check minVersions <= maxVerions
      if (hcd.getMinVersions() > hcd.getMaxVersions()) {
        String message = "Min versions for column family " + hcd.getNameAsString()
          + " must be less than the Max versions.";
        warnOrThrowExceptionForFailure(logWarn, message, null);
      }

      // check data replication factor, it can be 0(default value) when user has not explicitly
      // set the value, in this case we use default replication factor set in the file system.
      if (hcd.getDFSReplication() < 0) {
        String message = "HFile Replication for column family " + hcd.getNameAsString()
          + "  must be greater than zero.";
        warnOrThrowExceptionForFailure(logWarn, message, null);
      }

      // check in-memory compaction
if(KnobRuntime.check(java.util.UUID.fromString("1f990d90-a617-30a6-8d52-35bb79bfe3fe"))) {
logWarn = true;
}
if(KnobRuntime.check(java.util.UUID.fromString("90f2b78c-22ec-3150-b02c-ea2e79c62626"))) {
logWarn = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("3e139998-f5ef-36d3-a0fe-17f185bc1315"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("769884f7-e242-3058-aab6-c9767bc5db36"))) { warnOrThrowExceptionForFailure(false, hcd::getInMemoryCompaction); } else if (KnobRuntime.check(java.util.UUID.fromString("fcbaa528-f2c4-3525-8b77-47ce53e08d68"))) { warnOrThrowExceptionForFailure(!logWarn, hcd::getInMemoryCompaction); } else { warnOrThrowExceptionForFailure(logWarn, hcd::getInMemoryCompaction); }
    }
  }

  private static void checkReplicationScope(final Configuration conf, final TableDescriptor td)
    throws IOException {
    warnOrThrowExceptionForFailure(conf, () -> {
      for (ColumnFamilyDescriptor cfd : td.getColumnFamilies()) {
        // check replication scope
        WALProtos.ScopeType scop = WALProtos.ScopeType.valueOf(cfd.getScope());
        if (scop == null) {
          String message = "Replication scope for column family " + cfd.getNameAsString() + " is "
            + cfd.getScope() + " which is invalid.";

          throw new DoNotRetryIOException(message);
        }
      }
    });
  }

  private static void checkCompactionPolicy(final Configuration conf, final TableDescriptor td)
    throws IOException {
    warnOrThrowExceptionForFailure(false, () -> {
      // FIFO compaction has some requirements
      // Actually FCP ignores periodic major compactions
      String className = td.getValue(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY);
      if (className == null) {
        className = conf.get(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY,
          ExploringCompactionPolicy.class.getName());
      }

      int blockingFileCount = HStore.DEFAULT_BLOCKING_STOREFILE_COUNT;
      String sv = td.getValue(HStore.BLOCKING_STOREFILES_KEY);
      if (sv != null) {
if(KnobRuntime.check(java.util.UUID.fromString("9aa5165e-e98a-37d5-bf90-10648ede6142"))) {
throw new java.lang.NumberFormatException("Injected exception");
}
        blockingFileCount = Integer.parseInt(sv);
      } else {
        blockingFileCount = conf.getInt(HStore.BLOCKING_STOREFILES_KEY, blockingFileCount);
      }

      for (ColumnFamilyDescriptor hcd : td.getColumnFamilies()) {
        String compactionPolicy =
          hcd.getConfigurationValue(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY);
        if (compactionPolicy == null) {
          compactionPolicy = className;
        }
        if (!compactionPolicy.equals(FIFOCompactionPolicy.class.getName())) {
          continue;
        }
        // FIFOCompaction
        String message = null;

        // 1. Check TTL
        if (hcd.getTimeToLive() == ColumnFamilyDescriptorBuilder.DEFAULT_TTL) {
          message = "Default TTL is not supported for FIFO compaction";
          throw new IOException(message);
        }

        // 2. Check min versions
        if (hcd.getMinVersions() > 0) {
          message = "MIN_VERSION > 0 is not supported for FIFO compaction";
          throw new IOException(message);
        }

        // 3. blocking file count
        sv = hcd.getConfigurationValue(HStore.BLOCKING_STOREFILES_KEY);
        if (sv != null) {
          blockingFileCount = Integer.parseInt(sv);
        }
        if (blockingFileCount < 1000) {
          message =
            "Blocking file count '" + HStore.BLOCKING_STOREFILES_KEY + "' " + blockingFileCount
              + " is below recommended minimum of 1000 for column family " + hcd.getNameAsString();
          throw new IOException(message);
        }
      }
    });
  }

  private static void checkBloomFilterType(final Configuration conf, final TableDescriptor td)
    throws IOException {
    warnOrThrowExceptionForFailure(conf, () -> {
      for (ColumnFamilyDescriptor cfd : td.getColumnFamilies()) {
        Configuration cfdConf = new CompoundConfiguration().addStringMap(cfd.getConfiguration());
        try {
          BloomFilterUtil.getBloomFilterParam(cfd.getBloomFilterType(), cfdConf);
        } catch (IllegalArgumentException e) {
          throw new DoNotRetryIOException("Failed to get bloom filter param", e);
        }
      }
    });
  }

  public static void checkCompression(final Configuration conf, final TableDescriptor td)
    throws IOException {
    warnOrThrowExceptionForFailure(conf, () -> {
      for (ColumnFamilyDescriptor cfd : td.getColumnFamilies()) {
        CompressionTest.testCompression(cfd.getCompressionType());
        CompressionTest.testCompression(cfd.getCompactionCompressionType());
        CompressionTest.testCompression(cfd.getMajorCompactionCompressionType());
        CompressionTest.testCompression(cfd.getMinorCompactionCompressionType());
      }
    });
  }

  public static void checkEncryption(final Configuration conf, final TableDescriptor td)
    throws IOException {
    warnOrThrowExceptionForFailure(conf, () -> {
      for (ColumnFamilyDescriptor cfd : td.getColumnFamilies()) {
        EncryptionTest.testEncryption(conf, cfd.getEncryptionType(), cfd.getEncryptionKey());
      }
    });
  }

  public static void checkClassLoading(final Configuration conf, final TableDescriptor td)
    throws IOException {
    warnOrThrowExceptionForFailure(conf, () -> {
      RegionSplitPolicy.getSplitPolicyClass(td, conf);
      RegionCoprocessorHost.testTableCoprocessorAttrs(conf, td);
    });
  }

  // HBASE-13350 - Helper method to log warning on sanity check failures if checks disabled.
  private static void warnOrThrowExceptionForFailure(boolean logWarn, String message,
    Exception cause) throws IOException {
    if (!logWarn) {
      throw new DoNotRetryIOException(message + " Set " + TABLE_SANITY_CHECKS
        + " to false at conf or table descriptor if you want to bypass sanity checks", cause);
    }
    if (KnobRuntime.check(java.util.UUID.fromString("f58b72eb-a7d9-316d-8f60-f03710d9feb0"))) { LOG.warn(message + " Set "); } else { LOG.warn(message); }
  }

  private static void warnOrThrowExceptionForFailure(Configuration conf, ThrowingRunnable runnable)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("024478e2-2208-3692-87b2-f7cc0b83334e"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("362e809a-0aa8-3bfe-95e6-f6d52832b588"))) {
try {
    java.lang.reflect.Field field = conf.getClass().getDeclaredField("loadDefaults");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(conf);
    field.set(conf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ff882773-eab2-32fb-95fb-8e9fe2187c91"))) {
throw new java.io.IOException("Injected exception");
}
    boolean logWarn = !shouldSanityCheck(conf);
    warnOrThrowExceptionForFailure(logWarn, runnable);
  }

  private static void warnOrThrowExceptionForFailure(boolean logWarn, ThrowingRunnable runnable)
    throws IOException {
    try {
      runnable.run();
    } catch (Exception e) {
if(KnobRuntime.check(java.util.UUID.fromString("1cb70fa9-b5ad-342c-9d4e-4b61fc7aff1d"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("6f629669-dff6-3da5-a8dc-6c4df48cf90f"))) {
logWarn = false;
}
if(KnobRuntime.check(java.util.UUID.fromString("79cc6f30-bc8f-3bba-a8fe-d52e98e29cea"))) {
logWarn = true;
}
      if (KnobRuntime.check(java.util.UUID.fromString("bcf2ad70-2776-3997-b2d4-eec6ff6f53d5"))) { warnOrThrowExceptionForFailure(!logWarn, e.getMessage(), e); } else { warnOrThrowExceptionForFailure(logWarn, e.getMessage(), e); }
    }
  }

  @FunctionalInterface
  interface ThrowingRunnable {
    void run() throws Exception;
  }
}
