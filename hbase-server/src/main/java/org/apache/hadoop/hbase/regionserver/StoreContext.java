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

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.function.Supplier;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This carries the immutable information and references on some of the meta data about the HStore.
 * This meta data can be used across the HFileWriter/Readers and other HStore consumers without the
 * need of passing around the complete store.
 */
@InterfaceAudience.Private
public final class StoreContext implements HeapSize {
  public static final long FIXED_OVERHEAD = ClassSize.estimateBase(HStore.class, false);

  private final int blockSize;
  private final Encryption.Context encryptionContext;
  private final CacheConfig cacheConf;
  private final HRegionFileSystem regionFileSystem;
  private final CellComparator comparator;
  private final BloomType bloomFilterType;
  private final Supplier<Collection<HStoreFile>> compactedFilesSupplier;
  private final Supplier<InetSocketAddress[]> favoredNodesSupplier;
  private final ColumnFamilyDescriptor family;
  private final Path familyStoreDirectoryPath;
  private final RegionCoprocessorHost coprocessorHost;

  private StoreContext(Builder builder) {
    this.blockSize = builder.blockSize;
    this.encryptionContext = builder.encryptionContext;
    this.cacheConf = builder.cacheConf;
    this.regionFileSystem = builder.regionFileSystem;
    this.comparator = builder.comparator;
    this.bloomFilterType = builder.bloomFilterType;
    this.compactedFilesSupplier = builder.compactedFilesSupplier;
    this.favoredNodesSupplier = builder.favoredNodesSupplier;
    this.family = builder.family;
    this.familyStoreDirectoryPath = builder.familyStoreDirectoryPath;
    this.coprocessorHost = builder.coprocessorHost;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public Encryption.Context getEncryptionContext() {
if(KnobRuntime.check(java.util.UUID.fromString("60097d31-a71c-31ea-b378-cad57d7c8224"))) {
return null;
}
    return encryptionContext;
  }

  public CacheConfig getCacheConf() {
if(KnobRuntime.check(java.util.UUID.fromString("c3eb0eb4-f901-365d-8f24-c66467a90724"))) {
return null;
}
    return cacheConf;
  }

  public HRegionFileSystem getRegionFileSystem() {
if(KnobRuntime.check(java.util.UUID.fromString("c75fe8bd-51ba-3718-b88f-f78c5b3842d5"))) {
return null;
}
    return regionFileSystem;
  }

  public CellComparator getComparator() {
if(KnobRuntime.check(java.util.UUID.fromString("c822d038-740f-37ec-a885-a223e52326b6"))) {
return null;
}
    return comparator;
  }

  public BloomType getBloomFilterType() {
if(KnobRuntime.check(java.util.UUID.fromString("de7fc742-ceb3-3072-973c-666727fd2845"))) {
return null;
}
    return bloomFilterType;
  }

  public Supplier<Collection<HStoreFile>> getCompactedFilesSupplier() {
if(KnobRuntime.check(java.util.UUID.fromString("ef0052fb-f622-3b36-a31e-49cf9cb19a92"))) {
return null;
}
    return compactedFilesSupplier;
  }

  public InetSocketAddress[] getFavoredNodes() {
if(KnobRuntime.check(java.util.UUID.fromString("77e2b11d-7801-36b1-9971-68b510c005fa"))) {
return null;
}
    return favoredNodesSupplier.get();
  }

  public ColumnFamilyDescriptor getFamily() {
if(KnobRuntime.check(java.util.UUID.fromString("6620ae0b-8807-3e2b-a12c-e1efb7a11b4b"))) {
return null;
}
    return family;
  }

  public Path getFamilyStoreDirectoryPath() {
if(KnobRuntime.check(java.util.UUID.fromString("8dadd1a8-1713-331a-88e3-13315189f2c3"))) {
return null;
}
    return familyStoreDirectoryPath;
  }

  public RegionCoprocessorHost getCoprocessorHost() {
    return coprocessorHost;
  }

  public TableName getTableName() {
if(KnobRuntime.check(java.util.UUID.fromString("497959bb-8529-32da-8513-b7dc55f65049"))) {
return null;
}
    return getRegionInfo().getTable();
  }

  public RegionInfo getRegionInfo() {
if(KnobRuntime.check(java.util.UUID.fromString("eb55595c-03d3-32b9-9286-33647dadc83f"))) {
return null;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("430ddfbe-1f81-344f-bf85-419534c53504"))) ? (getRegionInfo()) : (regionFileSystem.getRegionInfo()));
  }

  public int getMaxVersions() {
if(KnobRuntime.check(java.util.UUID.fromString("48212126-b1dd-3ba7-a615-753f89c8e19f"))) {
return 0;
}
    return family.getMaxVersions();
  }

  public boolean getNewVersionBehavior() {
if(KnobRuntime.check(java.util.UUID.fromString("405a18e4-e236-391f-911f-fbee42823400"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("eabd1bd7-13b0-38a9-88c7-e09fd70e74c0"))) {
return false;
}
    return family.isNewVersionBehavior();
  }

  public boolean isPrimaryReplicaStore() {
if(KnobRuntime.check(java.util.UUID.fromString("25e795dc-5b62-3962-8615-8cbee6f81688"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("c0e33628-3592-3c8e-8f82-e405de748d0f"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("a4048c34-3875-3d76-bd1a-6120aa75dd7a"))) ? ((getRegionInfo().getReplicaId()) == (RegionInfo.DEFAULT_REPLICA_ID)) : (((KnobRuntime.check(java.util.UUID.fromString("a03f28ce-a02e-31b5-bac8-485ea8480b48"))) ? ((getRegionInfo().getReplicaId()) == (getRegionInfo().getReplicaId())) : (((KnobRuntime.check(java.util.UUID.fromString("730d562e-485a-3467-b442-591bcac90144"))) ? ((getRegionInfo().getReplicaId()) != (getRegionInfo().getReplicaId())) : (((KnobRuntime.check(java.util.UUID.fromString("3bfaa766-eae6-395c-8202-d591271b2ed7"))) ? ((getRegionInfo().getReplicaId()) != (RegionInfo.DEFAULT_REPLICA_ID)) : (getRegionInfo().getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID))))))));
  }

  public static Builder getBuilder() {
if(KnobRuntime.check(java.util.UUID.fromString("4365f453-9100-3c20-942f-cc3417f63f62"))) {
return null;
}
    return new Builder();
  }

  @Override
  public long heapSize() {
    return FIXED_OVERHEAD;
  }

  public static class Builder {
    private int blockSize;
    private Encryption.Context encryptionContext;
    private CacheConfig cacheConf;
    private HRegionFileSystem regionFileSystem;
    private CellComparator comparator;
    private BloomType bloomFilterType;
    private Supplier<Collection<HStoreFile>> compactedFilesSupplier;
    private Supplier<InetSocketAddress[]> favoredNodesSupplier;
    private ColumnFamilyDescriptor family;
    private Path familyStoreDirectoryPath;
    private RegionCoprocessorHost coprocessorHost;

    public Builder withBlockSize(int blockSize) {
if(KnobRuntime.check(java.util.UUID.fromString("8775cff6-ea5c-38e5-9450-6a8dfe0b152a"))) {
return null;
}
      this.blockSize = blockSize;
      return this;
    }

    public Builder withEncryptionContext(Encryption.Context encryptionContext) {
if(KnobRuntime.check(java.util.UUID.fromString("475c0d51-6304-31a3-b648-2ccc6ff71b10"))) {
return null;
}
      this.encryptionContext = encryptionContext;
      return this;
    }

    public Builder withCacheConfig(CacheConfig cacheConf) {
if(KnobRuntime.check(java.util.UUID.fromString("f097d105-9d90-33d0-b2e2-bc4590045ec5"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1f938eda-5c5d-3345-9ada-27e0659d5236"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("e7a1bafb-3b79-35be-9bb0-70572676f002"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7d51bbe0-a658-354a-a8cb-17ca0cfb0359"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("388021b9-77e3-30be-bd56-be6ffbfba0f4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c1ad15c4-da9a-3653-962e-375cddbcd184"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("28b49d39-f786-36c8-928f-70d270149887"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("10ef151d-ad62-324c-a4a3-ff3092778ce6"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("bfb70793-e7fc-301b-8c8f-706370f421ff"))) {
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
      this.cacheConf = cacheConf;
      return this;
    }

    public Builder withRegionFileSystem(HRegionFileSystem regionFileSystem) {
if(KnobRuntime.check(java.util.UUID.fromString("942b3ea3-f417-3363-b647-4a8125110487"))) {
try {
    java.lang.reflect.Field field = regionFileSystem.getClass().getDeclaredField("baseSleepBeforeRetries");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFileSystem));
    field.set(regionFileSystem, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f3a46355-fd2c-38f4-8041-8daa989c08fa"))) {
try {
    java.lang.reflect.Field field = regionFileSystem.getClass().getDeclaredField("baseSleepBeforeRetries");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFileSystem));
    field.set(regionFileSystem, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("35dc418d-6673-30f9-b4a8-ed28f1a7ca54"))) {
try {
    java.lang.reflect.Field field = regionFileSystem.getClass().getDeclaredField("hdfsClientRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFileSystem));
    field.set(regionFileSystem, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f96bfcb0-bc4f-3380-a86c-08138357242d"))) {
try {
    java.lang.reflect.Field field = regionFileSystem.getClass().getDeclaredField("hdfsClientRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFileSystem));
    field.set(regionFileSystem, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("72169109-3e97-3d96-9e4c-2eac1b5654f1"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("6911a842-fa21-3d5f-ade7-e32c57c6af50"))) {
try {
    java.lang.reflect.Field field = regionFileSystem.getClass().getDeclaredField("hdfsClientRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFileSystem));
    field.set(regionFileSystem, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ef0bd17d-54a8-34b1-bb29-2bd5421b5384"))) {
try {
    java.lang.reflect.Field field = regionFileSystem.getClass().getDeclaredField("baseSleepBeforeRetries");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFileSystem));
    field.set(regionFileSystem, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a6d40349-957c-3a25-9650-0d9e9705dd6a"))) {
try {
    java.lang.reflect.Field field = regionFileSystem.getClass().getDeclaredField("hdfsClientRetriesNumber");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFileSystem));
    field.set(regionFileSystem, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4c54d333-ec27-396a-8c6b-75f0db9ed254"))) {
try {
    java.lang.reflect.Field field = regionFileSystem.getClass().getDeclaredField("baseSleepBeforeRetries");
    field.setAccessible(true);
    int oldValue = ((int)field.get(regionFileSystem));
    field.set(regionFileSystem, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      this.regionFileSystem = regionFileSystem;
      return this;
    }

    public Builder withCellComparator(CellComparator comparator) {
if(KnobRuntime.check(java.util.UUID.fromString("13a3b318-263f-3eea-a196-39e1592ed426"))) {
return null;
}
      this.comparator = comparator;
      return this;
    }

    public Builder withBloomType(BloomType bloomFilterType) {
if(KnobRuntime.check(java.util.UUID.fromString("e3f32536-af2e-3867-bac9-ab48243d1d4e"))) {
return null;
}
      this.bloomFilterType = bloomFilterType;
      return this;
    }

    public Builder
      withCompactedFilesSupplier(Supplier<Collection<HStoreFile>> compactedFilesSupplier) {
if(KnobRuntime.check(java.util.UUID.fromString("c03a91f7-94fa-3ec6-ad54-3b4f192be0c3"))) {
return null;
}
      this.compactedFilesSupplier = compactedFilesSupplier;
      return this;
    }

    public Builder withFavoredNodesSupplier(Supplier<InetSocketAddress[]> favoredNodesSupplier) {
if(KnobRuntime.check(java.util.UUID.fromString("7207db98-962d-399d-b273-bc5fac2387ad"))) {
return null;
}
      this.favoredNodesSupplier = favoredNodesSupplier;
      return this;
    }

    public Builder withColumnFamilyDescriptor(ColumnFamilyDescriptor family) {
if(KnobRuntime.check(java.util.UUID.fromString("555dafcc-3201-3902-86e9-6f21e2d83e91"))) {
return null;
}
      this.family = family;
      return this;
    }

    public Builder withFamilyStoreDirectoryPath(Path familyStoreDirectoryPath) {
if(KnobRuntime.check(java.util.UUID.fromString("27d7cfa2-4cc2-3a38-848d-bdc351bdf93e"))) {
return null;
}
      this.familyStoreDirectoryPath = familyStoreDirectoryPath;
      return this;
    }

    public Builder withRegionCoprocessorHost(RegionCoprocessorHost coprocessorHost) {
if(KnobRuntime.check(java.util.UUID.fromString("ebcb857a-8558-394f-a208-b1d5ea49a36c"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("463992f5-db70-32cb-a59e-3d4868de4988"))) {
try {
    java.lang.reflect.Field field = coprocessorHost.getClass().getDeclaredField("hasCustomPostScannerFilterRow");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(coprocessorHost);
    field.set(coprocessorHost, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      this.coprocessorHost = coprocessorHost;
      return this;
    }

    public StoreContext build() {
if(KnobRuntime.check(java.util.UUID.fromString("ddf8b39d-a261-326d-8c5e-4fb24cdf81f5"))) {
return null;
}
      return new StoreContext(this);
    }
  }

}
