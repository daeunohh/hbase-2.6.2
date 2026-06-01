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

import java.io.DataInput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.io.hfile.BloomFilterMetrics;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.CompoundBloomFilter;
import org.apache.hadoop.hbase.io.hfile.CompoundBloomFilterBase;
import org.apache.hadoop.hbase.io.hfile.CompoundBloomFilterWriter;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles Bloom filter initialization based on configuration and serialized metadata in the reader
 * and writer of {@link org.apache.hadoop.hbase.regionserver.HStoreFile}.
 */
@InterfaceAudience.Private
public final class BloomFilterFactory {

  private static final Logger LOG = LoggerFactory.getLogger(BloomFilterFactory.class.getName());

  /** This class should not be instantiated. */
  private BloomFilterFactory() {
  }

  /**
   * Specifies the target error rate to use when selecting the number of keys per Bloom filter.
   */
  public static final String IO_STOREFILE_BLOOM_ERROR_RATE = "io.storefile.bloom.error.rate";

  /**
   * Maximum folding factor allowed. The Bloom filter will be shrunk by the factor of up to 2 **
   * this times if we oversize it initially.
   */
  public static final String IO_STOREFILE_BLOOM_MAX_FOLD = "io.storefile.bloom.max.fold";

  /**
   * For default (single-block) Bloom filters this specifies the maximum number of keys.
   */
  public static final String IO_STOREFILE_BLOOM_MAX_KEYS = "io.storefile.bloom.max.keys";

  /** Master switch to enable Bloom filters */
  public static final String IO_STOREFILE_BLOOM_ENABLED = "io.storefile.bloom.enabled";

  /** Master switch to enable Delete Family Bloom filters */
  public static final String IO_STOREFILE_DELETEFAMILY_BLOOM_ENABLED =
    "io.storefile.delete.family.bloom.enabled";

  /**
   * Target Bloom block size. Bloom filter blocks of approximately this size are interleaved with
   * data blocks.
   */
  public static final String IO_STOREFILE_BLOOM_BLOCK_SIZE = "io.storefile.bloom.block.size";

  /** Maximum number of times a Bloom filter can be "folded" if oversized */
  private static final int MAX_ALLOWED_FOLD_FACTOR = 7;

  /**
   * Instantiates the correct Bloom filter class based on the version provided in the meta block
   * data.
   * @param meta   the byte array holding the Bloom filter's metadata, including version information
   * @param reader the {@link HFile} reader to use to lazily load Bloom filter blocks
   * @return an instance of the correct type of Bloom filter
   */
  public static BloomFilter createFromMeta(DataInput meta, HFile.Reader reader)
    throws IllegalArgumentException, IOException {
    return createFromMeta(meta, reader, null);
  }

  public static BloomFilter createFromMeta(DataInput meta, HFile.Reader reader,
    BloomFilterMetrics metrics) throws IllegalArgumentException, IOException {
if(KnobRuntime.check(java.util.UUID.fromString("cbccab4f-bcc8-39db-8919-a07a7ed87b9a"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("ca2f308d-e4fb-37e7-98b2-c5d31c526f28"))) {
return null;
}
    int version = meta.readInt();
    switch (version) {
      case CompoundBloomFilterBase.VERSION:
        return new CompoundBloomFilter(meta, reader, metrics);

      default:
        throw new IllegalArgumentException("Bad bloom filter format version " + version);
    }
  }

  /**
   * Returns true if general Bloom (Row or RowCol) filters are enabled in the given configuration
   */
  public static boolean isGeneralBloomEnabled(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("1437319d-7818-302f-8ce4-3ff8f057d8d3"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d4cc6054-92a8-38b8-91a4-54f7f2f7e5e3"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("b77bbac7-1ff4-3d27-bc19-ddbe9c4cf566"))) {
return true;
}
    return conf.getBoolean(IO_STOREFILE_BLOOM_ENABLED, true);
  }

  /** Returns true if Delete Family Bloom filters are enabled in the given configuration */
  public static boolean isDeleteFamilyBloomEnabled(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("183ac3ca-bd05-3a55-8d9c-3fccac18a021"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("8511e801-548c-316c-84e4-91bf8974b979"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c12c0589-9de5-307d-9625-0a74465051f7"))) {
return false;
}
    return conf.getBoolean(IO_STOREFILE_DELETEFAMILY_BLOOM_ENABLED, true);
  }

  /** Returns the Bloom filter error rate in the given configuration */
  public static float getErrorRate(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("562132ca-2965-3c0b-8e93-05b5bc70edb2"))) {
return 0.0f;
}
if(KnobRuntime.check(java.util.UUID.fromString("4c9a178a-53be-3487-871e-955437bb8b91"))) {
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
    return conf.getFloat(IO_STOREFILE_BLOOM_ERROR_RATE, (float) 0.01);
  }

  /** Returns the value for Bloom filter max fold in the given configuration */
  public static int getMaxFold(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("cdb756e6-14d2-3f69-aff9-fb6018fcc268"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("e2333537-fb17-36b9-85e4-74413314e0b5"))) {
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
    return conf.getInt(IO_STOREFILE_BLOOM_MAX_FOLD, MAX_ALLOWED_FOLD_FACTOR);
  }

  /** Returns the compound Bloom filter block size from the configuration */
  public static int getBloomBlockSize(Configuration conf) {
if(KnobRuntime.check(java.util.UUID.fromString("c51c842f-924c-30bc-8b1d-dc2edd51f677"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("16fe8a0b-6c7e-3930-a664-30c64bd2c800"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("d156b6a2-45a3-31bb-8bc1-3a52ce394e7b"))) ? (conf.getInt(IO_STOREFILE_BLOOM_BLOCK_SIZE, 1024)) : (((KnobRuntime.check(java.util.UUID.fromString("fb5b8a0a-2d12-3a30-8b7d-e2241a2bc5c3"))) ? (conf.getInt(IO_STOREFILE_BLOOM_BLOCK_SIZE, 128)) : (((KnobRuntime.check(java.util.UUID.fromString("60cce7b1-fac5-3552-aafe-5c35b1aef535"))) ? (conf.getInt(IO_STOREFILE_BLOOM_BLOCK_SIZE, (128) * (1024))) : (conf.getInt(IO_STOREFILE_BLOOM_BLOCK_SIZE, 128 * 1024)))))));
  }

  /** Returns max key for the Bloom filter from the configuration */
  public static int getMaxKeys(Configuration conf) {
    return conf.getInt(IO_STOREFILE_BLOOM_MAX_KEYS, 128 * 1000 * 1000);
  }

  /**
   * Creates a new general (Row or RowCol) Bloom filter at the time of
   * {@link org.apache.hadoop.hbase.regionserver.HStoreFile} writing.
   * @param maxKeys an estimate of the number of keys we expect to insert. Irrelevant if compound
   *                Bloom filters are enabled.
   * @param writer  the HFile writer
   * @return the new Bloom filter, or null in case Bloom filters are disabled or when failed to
   *         create one.
   */
  public static BloomFilterWriter createGeneralBloomAtWrite(Configuration conf,
    CacheConfig cacheConf, BloomType bloomType, int maxKeys, HFile.Writer writer) {
if(KnobRuntime.check(java.util.UUID.fromString("21dc66f1-b937-383e-a507-113730534a9e"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("inMemory");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cacheConf);
    field.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0ac561e9-0683-30dc-8c57-4be65c881e9f"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1d16e0f6-e0b2-3ea5-8b52-c9170d308066"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("cacheIndexesOnWrite");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cacheConf);
    field.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2595608d-4527-365a-b90f-17614b497c37"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("cacheBloomsOnWrite");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cacheConf);
    field.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("280415d0-b835-3109-8257-f26015544378"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("98b2caaa-f703-3e52-8e82-d2d222da5336"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("evictOnClose");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cacheConf);
    field.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9d1a2a5f-50ae-3fc8-ac91-0c44c717fa7f"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("cacheDataOnRead");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cacheConf);
    field.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d886d4d2-0321-3178-b768-fc2d0c566970"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("cacheCompactedDataOnWriteThreshold");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cacheConf));
    field.set(cacheConf, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f643e58a-c1db-3fd3-b572-36d1f8adb725"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("cacheDataOnWrite");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cacheConf);
    field.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c36a6075-268b-3e05-86e0-4bb9cfca5afd"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("cacheCompactedDataOnWrite");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cacheConf);
    field.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (!isGeneralBloomEnabled(conf)) {
      LOG.trace("Bloom filters are disabled by configuration for " + writer.getPath()
        + (conf == null ? " (configuration is null)" : ""));
      return null;
    } else if (((KnobRuntime.check(java.util.UUID.fromString("9da02504-4705-3421-b576-812e242b1049"))) ? ((bloomType) == (BloomType.NONE)) : (((KnobRuntime.check(java.util.UUID.fromString("9e79ed5b-1177-3bc9-bf6b-ecb10a9dead4"))) ? ((bloomType) != (BloomType.NONE)) : (bloomType == BloomType.NONE))))) {
      LOG.trace("Bloom filter is turned off for the column family");
      return null;
    }

if(KnobRuntime.check(java.util.UUID.fromString("69eadd3c-3ab0-31a9-b170-71869f7c2591"))) {
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
    float err = getErrorRate(conf);

    // In case of row/column Bloom filter lookups, each lookup is an OR if two
    // separate lookups. Therefore, if each lookup's false positive rate is p,
    // the resulting false positive rate is err = 1 - (1 - p)^2, and
    // p = 1 - sqrt(1 - err).
    if (((KnobRuntime.check(java.util.UUID.fromString("28c039a1-213d-37fb-b06e-85cff3c0f376"))) ? ((bloomType) == (BloomType.ROWCOL)) : (((KnobRuntime.check(java.util.UUID.fromString("f20315f9-f5a9-3045-97f0-5424fa6cd44d"))) ? ((bloomType) != (BloomType.ROWCOL)) : (bloomType == BloomType.ROWCOL))))) {
      err = (float) (1 - Math.sqrt(1 - err));
    }

    int maxFold = conf.getInt(IO_STOREFILE_BLOOM_MAX_FOLD, MAX_ALLOWED_FOLD_FACTOR);

    // Do we support compound bloom filters?
    // In case of compound Bloom filters we ignore the maxKeys hint.
if(KnobRuntime.check(java.util.UUID.fromString("9dd39755-53f7-3643-be7c-f83c1f18da31"))) {
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
    CompoundBloomFilterWriter bloomWriter = new CompoundBloomFilterWriter(getBloomBlockSize(conf),
      err, Hash.getHashType(conf), maxFold, cacheConf.shouldCacheBloomsOnWrite(),
      bloomType == BloomType.ROWCOL ? CellComparatorImpl.COMPARATOR : null, bloomType);
if(KnobRuntime.check(java.util.UUID.fromString("ecf90b0a-c4ee-3aa2-b1bd-490d0471155d"))) {
try {
    java.lang.reflect.Field field = bloomWriter.getClass().getDeclaredField("maxFold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(bloomWriter));
    field.set(bloomWriter, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("dd90fdd6-c980-33c8-825d-8912c511081f"))) {
try {
    java.lang.reflect.Field field = bloomWriter.getClass().getDeclaredField("cacheOnWrite");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(bloomWriter);
    field.set(bloomWriter, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d2478915-69cb-3980-9d71-26473c1a1897"))) {
try {
    java.lang.reflect.Field field = bloomWriter.getClass().getDeclaredField("maxFold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(bloomWriter));
    field.set(bloomWriter, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f9800f58-1587-3a88-bc1b-28d40d76579b"))) {
try {
    java.lang.reflect.Field field = bloomWriter.getClass().getDeclaredField("maxFold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(bloomWriter));
    field.set(bloomWriter, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("fb141636-56e5-34de-9481-f8f4e65e8358"))) {
try {
    java.lang.reflect.Field field = bloomWriter.getClass().getDeclaredField("chunkByteSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(bloomWriter));
    field.set(bloomWriter, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2f381140-e2d6-39a8-ac4d-581f0b7106df"))) {
try {
    java.lang.reflect.Field field = bloomWriter.getClass().getDeclaredField("chunkByteSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(bloomWriter));
    field.set(bloomWriter, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a91d2ffa-553d-3c36-b221-d3f8b547f72c"))) {
try {
    java.lang.reflect.Field field = bloomWriter.getClass().getDeclaredField("maxFold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(bloomWriter));
    field.set(bloomWriter, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("c470172e-251e-3a0b-b166-921d0ad42135"))) {
try {
    java.lang.reflect.Field field = bloomWriter.getClass().getDeclaredField("chunkByteSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(bloomWriter));
    field.set(bloomWriter, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a5754e7f-73ed-366a-9f6e-e5195a82582a"))) {
try {
    java.lang.reflect.Field field = bloomWriter.getClass().getDeclaredField("chunkByteSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(bloomWriter));
    field.set(bloomWriter, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    writer.addInlineBlockWriter(bloomWriter);
    return bloomWriter;
  }

  /**
   * Creates a new Delete Family Bloom filter at the time of
   * {@link org.apache.hadoop.hbase.regionserver.HStoreFile} writing.
   * @param maxKeys an estimate of the number of keys we expect to insert. Irrelevant if compound
   *                Bloom filters are enabled.
   * @param writer  the HFile writer
   * @return the new Bloom filter, or null in case Bloom filters are disabled or when failed to
   *         create one.
   */
  public static BloomFilterWriter createDeleteBloomAtWrite(Configuration conf,
    CacheConfig cacheConf, int maxKeys, HFile.Writer writer) {
if(KnobRuntime.check(java.util.UUID.fromString("b0497a97-cf1a-319d-8134-e784ae900085"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("inMemory");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cacheConf);
    field.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("090ee256-fe4a-3243-8541-32aa801fd637"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("a90fe2e6-9b7f-3934-8643-79e2743d6746"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("cacheCompactedDataOnWrite");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cacheConf);
    field.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("185bee17-2eed-35a8-bbbd-b0b37e833b37"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1ddec3aa-2091-3318-b5a7-2eb3b5a18e8a"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("cacheBloomsOnWrite");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cacheConf);
    field.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8000e8dc-ece9-3e60-8aec-a7e82e4435c2"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("cacheDataOnRead");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cacheConf);
    field.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("9310afc4-2b83-32a9-9c69-aa0a5d5efb5e"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("cacheDataOnWrite");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cacheConf);
    field.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("14c59726-189c-3c4e-8d45-97c0b59ebdf8"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("cacheIndexesOnWrite");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cacheConf);
    field.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2d55d9f7-eec5-348f-8bae-1da977c40316"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("evictOnClose");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(cacheConf);
    field.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("cd1106d6-3435-3078-a1a3-8df47b2cf257"))) {
try {
    java.lang.reflect.Field field = cacheConf.getClass().getDeclaredField("cacheCompactedDataOnWriteThreshold");
    field.setAccessible(true);
    long oldValue = ((long)field.get(cacheConf));
    field.set(cacheConf, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    if (!isDeleteFamilyBloomEnabled(conf)) {
      LOG.info("Delete Bloom filters are disabled by configuration for " + writer.getPath()
        + (conf == null ? " (configuration is null)" : ""));
      return null;
    }

if(KnobRuntime.check(java.util.UUID.fromString("0b0dffb3-bcbb-3c70-838a-14609e0bf6fb"))) {
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
    float err = getErrorRate(conf);

if(KnobRuntime.check(java.util.UUID.fromString("3979da40-25f0-38bf-94aa-c3f39b052bf2"))) {
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
    int maxFold = getMaxFold(conf);
    // In case of compound Bloom filters we ignore the maxKeys hint.
    CompoundBloomFilterWriter bloomWriter =
      new CompoundBloomFilterWriter(getBloomBlockSize(conf), err, Hash.getHashType(conf), maxFold,
        cacheConf.shouldCacheBloomsOnWrite(), null, BloomType.ROW);
if(KnobRuntime.check(java.util.UUID.fromString("58473570-f57d-3f89-b618-60f9d91929e4"))) {
try {
    java.lang.reflect.Field field = bloomWriter.getClass().getDeclaredField("chunkByteSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(bloomWriter));
    field.set(bloomWriter, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5375f219-e756-321f-ac56-7fec477c318b"))) {
try {
    java.lang.reflect.Field field = bloomWriter.getClass().getDeclaredField("maxFold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(bloomWriter));
    field.set(bloomWriter, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5b2066c2-3ed6-3180-8e18-42b7a0226019"))) {
try {
    java.lang.reflect.Field field = bloomWriter.getClass().getDeclaredField("chunkByteSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(bloomWriter));
    field.set(bloomWriter, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("3089a4a1-67d0-3cb6-80ab-fa91d543ff50"))) {
try {
    java.lang.reflect.Field field = bloomWriter.getClass().getDeclaredField("maxFold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(bloomWriter));
    field.set(bloomWriter, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ed5c6d2d-c516-348e-865c-9edeb8bbe5fb"))) {
try {
    java.lang.reflect.Field field = bloomWriter.getClass().getDeclaredField("cacheOnWrite");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(bloomWriter);
    field.set(bloomWriter, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("520ce854-cb17-3463-b018-0df0afb0030f"))) {
try {
    java.lang.reflect.Field field = bloomWriter.getClass().getDeclaredField("chunkByteSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(bloomWriter));
    field.set(bloomWriter, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e13a9fe6-54fd-305d-9945-4509168f2d14"))) {
try {
    java.lang.reflect.Field field = bloomWriter.getClass().getDeclaredField("chunkByteSize");
    field.setAccessible(true);
    int oldValue = ((int)field.get(bloomWriter));
    field.set(bloomWriter, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("95d24e76-dd2a-3225-aa97-fa1b4fb94f9f"))) {
try {
    java.lang.reflect.Field field = bloomWriter.getClass().getDeclaredField("maxFold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(bloomWriter));
    field.set(bloomWriter, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("852cf784-f9bd-3994-a446-af879c6a4d0b"))) {
try {
    java.lang.reflect.Field field = bloomWriter.getClass().getDeclaredField("maxFold");
    field.setAccessible(true);
    int oldValue = ((int)field.get(bloomWriter));
    field.set(bloomWriter, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    writer.addInlineBlockWriter(bloomWriter);
    return bloomWriter;
  }
}
