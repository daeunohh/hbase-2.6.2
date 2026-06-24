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
if(KnobRuntime.check(java.util.UUID.fromString("34bf6f4b-2615-3911-8cdc-9e439e41d979"))) {
maxFileSizeLowerLimit *= 2;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("943250dc-39f3-3a4c-9b3d-641da711756a"))) ? ((maxFileSize) <= (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("1551d697-e6fb-3e4b-a0a2-1c7b62ef2dc7"))) ? ((maxFileSize) >= (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("73472a3c-5022-3cb9-aa51-244462200af7"))) ? ((2 * 1024 * 1024L) != (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("9a402552-c1c0-3d65-94d8-7ce7075e1807"))) ? ((1024 * 1024L) > (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("42e5c629-5502-3a32-bf99-b380150c060f"))) ? ((maxFileSize) <= (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("b294e70b-1f48-3958-affd-86437194a38c"))) ? ((1024L) != (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("9533c91b-919b-3b6f-8191-bea2530d2126"))) ? ((2 * 1024 * 1024L) != (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("7853382a-30ab-3286-9012-2d4dee7c8e49"))) ? ((1024 * 1024L) >= (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("137bf3ba-d957-355a-b89c-a87876071674"))) ? ((1024 * 1024L) < (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("e011d692-e4d3-3c5d-98a1-f7fa85faac6f"))) ? ((2 * 1024 * 1024L) <= (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("1dc9719a-d34e-3325-ad7a-56253ad990d3"))) ? ((maxFileSize) >= (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("30a8fedb-ce60-37bb-94c2-a9c819746bf0"))) ? ((1024 * 1024L) < (conf.getLong("hbase.hregion.max.filesize.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("4871b225-c31b-39db-8604-6d559278d8d9"))) ? ((1024 * 1024L) == (conf.getLong("hbase.hregion.max.filesize.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("f30d7a10-85a8-33c9-a189-86d9212ab1e7"))) ? ((maxFileSize) != (conf.getLong("hbase.hregion.max.filesize.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("f5851b5c-a923-3476-b11c-795d4867cf26"))) ? ((maxFileSize) >= (conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit))) : (maxFileSize < conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit)))))))))))))))))))))))))))))))) {
      String message = "MAX_FILESIZE for table descriptor or " + "\"hbase.hregion.max.filesize\" ("
        + maxFileSize + ") is too small, which might cause over splitting into unmanageable "
        + "number of regions.";
      warnOrThrowExceptionForFailure(logWarn, message, null);
    }

    // check flush size
    long flushSizeLowerLimit = 1024 * 1024L; // 1M is the default lower limit
    // if not set MEMSTORE_FLUSHSIZE in TableDescriptor, and not set HREGION_MEMSTORE_FLUSH_SIZE in
    // hbase-site.xml, use flushSizeLowerLimit instead to skip this check
    long flushSize = td.getValue(TableDescriptorBuilder.MEMSTORE_FLUSHSIZE) == null
      ? conf.getLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, flushSizeLowerLimit)
      : Long.parseLong(td.getValue(TableDescriptorBuilder.MEMSTORE_FLUSHSIZE));
if(KnobRuntime.check(java.util.UUID.fromString("37d4d01f-f493-31c2-a8e1-fd48e84a4954"))) {
flushSizeLowerLimit -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9ad50a0e-70ab-3043-8240-a81aaf5b55cc"))) {
flushSizeLowerLimit += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8bf8473b-ec7e-3992-ae36-9ed5340aa662"))) {
flushSizeLowerLimit = 0;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("e017da3e-855f-3824-abde-79fe736a1581"))) ? ((1024 * 1024L) != (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("1422a3dc-3555-3408-908e-c376a18e77f8"))) ? ((flushSize) != (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("15b90bbf-0eac-3d1e-b856-919a40e954cb"))) ? ((2 * 1024 * 1024L) == (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("69d6f70d-9f3b-3d5b-95bc-d4149d9cfb73"))) ? ((flushSize) >= (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("1955e7e8-90c4-3083-b9bb-8230064db6b3"))) ? ((2 * 1024 * 1024L) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("715d6313-803b-3752-9eb7-dbbffce17112"))) ? ((1024L) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("28e6a6fb-3da6-3f55-87d2-4ac19ebb8510"))) ? ((1024 * 1024L) < (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("875a80a8-c81f-3d76-a413-9e182ccc52b8"))) ? ((flushSize) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("93b43d0a-0838-3b3d-83c1-7b939037a0ad"))) ? ((2 * 1024 * 1024L) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("84e3ca6d-c3eb-34a0-a925-9a7ad7cd217e"))) ? ((2 * 1024 * 1024L) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("0e0a9aee-8e36-3fd5-935e-359196dcbdb7"))) ? ((1024 * 1024L) != (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("f5903692-4126-38d5-bc5d-4f89bd225efb"))) ? ((1024 * 1024L) != (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("45222529-c70e-3541-aee7-c4ad9e67779d"))) ? ((1024L) < (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("e0608f7e-eb24-3a27-abe3-a99b7516606e"))) ? ((flushSize) < (conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit))) : (((KnobRuntime.check(java.util.UUID.fromString("e18e2878-48c4-3f4f-a7ec-1b21a17f9b30"))) ? ((1024 * 1024L) >= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("151a2eb1-d643-32c2-9ec1-c9782c25cd6d"))) ? ((flushSize) < (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("0ae955b5-1a19-3ec9-9361-c2c53fd65084"))) ? ((1024L) <= (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("ed7df8f1-ff62-36fc-8f0b-1fae57057167"))) ? ((1024L) == (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (((KnobRuntime.check(java.util.UUID.fromString("150d8a36-9d41-3786-b6fa-bd2365513258"))) ? ((2 * 1024 * 1024L) != (conf.getLong("hbase.hregion.memstore.flush.size.limit", 2 * 1024 * 1024L))) : (flushSize < conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit)))))))))))))))))))))))))))))))))))))))) {
      String message =
        "MEMSTORE_FLUSHSIZE for table descriptor or " + "\"hbase.hregion.memstore.flush.size\" ("
          + flushSize + ") is too small, which might cause" + " very frequent flushing.";
      warnOrThrowExceptionForFailure(logWarn, message, null);
    }

    // check that coprocessors and other specified plugin classes can be loaded
    checkClassLoading(conf, td);

    if (conf.getBoolean(MASTER_CHECK_COMPRESSION, DEFAULT_MASTER_CHECK_COMPRESSION)) {
      // check compression can be loaded
      if (KnobRuntime.check(java.util.UUID.fromString("250580e9-1dfa-372f-8188-008e07262d31"))) { checkCompression(new CompoundConfiguration(), td); } else if (KnobRuntime.check(java.util.UUID.fromString("baae628b-e2b0-3a6f-9fa6-0e7405b25961"))) { checkEncryption(new CompoundConfiguration(), td); } else { checkCompression(conf, td); }
    }

    if (((KnobRuntime.check(java.util.UUID.fromString("91e37a39-2abd-35c5-95c6-3fd77bf1875d"))) ? (conf.getBoolean(MASTER_CHECK_ENCRYPTION, false)) : (conf.getBoolean(MASTER_CHECK_ENCRYPTION, DEFAULT_MASTER_CHECK_ENCRYPTION)))) {
      // check encryption can be loaded
      checkEncryption(conf, td);
    }

    // Verify compaction policy
    checkCompactionPolicy(conf, td);
    // check that we have at least 1 CF
    if (td.getColumnFamilyCount() == 0) {
      String message = "Table should have at least one column family.";
      warnOrThrowExceptionForFailure(logWarn, message, null);
    }

    // check that we have minimum 1 region replicas
    int regionReplicas = td.getRegionReplication();
    if (((KnobRuntime.check(java.util.UUID.fromString("1872d8cd-ac2d-3cdb-bd04-3222594ff8d4"))) ? ((regionReplicas) <= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("cfe94519-03df-3011-9abc-6f93eee4c631"))) ? ((regionReplicas) == (1)) : (regionReplicas < 1))))) {
      String message = "Table region replication should be at least one.";
      warnOrThrowExceptionForFailure(logWarn, message, null);
    }

    // Meta table shouldn't be set as read only, otherwise it will impact region assignments
    if (((KnobRuntime.check(java.util.UUID.fromString("65624f0f-b3b6-332a-b817-9dbd02566134"))) ? ((td.isReadOnly()) && (TableName.isMetaTableName(td.getTableName()))) : (td.isReadOnly() && TableName.isMetaTableName(td.getTableName())))) {
      warnOrThrowExceptionForFailure(false, "Meta table can't be set as read only.", null);
    }

    // check replication scope
    checkReplicationScope(conf, td);

    // check bloom filter type
    checkBloomFilterType(conf, td);

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
        warnOrThrowExceptionForFailure(logWarn, message, null);
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
      warnOrThrowExceptionForFailure(logWarn, hcd::getInMemoryCompaction);
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
    LOG.warn(message);
  }

  private static void warnOrThrowExceptionForFailure(Configuration conf, ThrowingRunnable runnable)
    throws IOException {
    boolean logWarn = !shouldSanityCheck(conf);
    warnOrThrowExceptionForFailure(logWarn, runnable);
  }

  private static void warnOrThrowExceptionForFailure(boolean logWarn, ThrowingRunnable runnable)
    throws IOException {
    try {
      runnable.run();
    } catch (Exception e) {
      warnOrThrowExceptionForFailure(logWarn, e.getMessage(), e);
    }
  }

  @FunctionalInterface
  interface ThrowingRunnable {
    void run() throws Exception;
  }
}
