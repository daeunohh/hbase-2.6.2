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
    return encryptionContext;
  }

  public CacheConfig getCacheConf() {
    return cacheConf;
  }

  public HRegionFileSystem getRegionFileSystem() {
    return regionFileSystem;
  }

  public CellComparator getComparator() {
    return comparator;
  }

  public BloomType getBloomFilterType() {
    return bloomFilterType;
  }

  public Supplier<Collection<HStoreFile>> getCompactedFilesSupplier() {
    return compactedFilesSupplier;
  }

  public InetSocketAddress[] getFavoredNodes() {
if(KnobRuntime.check(java.util.UUID.fromString("77e2b11d-7801-36b1-9971-68b510c005fa"))) {
return null;
}
    return favoredNodesSupplier.get();
  }

  public ColumnFamilyDescriptor getFamily() {
    return family;
  }

  public Path getFamilyStoreDirectoryPath() {
    return familyStoreDirectoryPath;
  }

  public RegionCoprocessorHost getCoprocessorHost() {
    return coprocessorHost;
  }

  public TableName getTableName() {
    return getRegionInfo().getTable();
  }

  public RegionInfo getRegionInfo() {
    return regionFileSystem.getRegionInfo();
  }

  public int getMaxVersions() {
    return family.getMaxVersions();
  }

  public boolean getNewVersionBehavior() {
    return family.isNewVersionBehavior();
  }

  public boolean isPrimaryReplicaStore() {
    return getRegionInfo().getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID;
  }

  public static Builder getBuilder() {
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
      this.blockSize = blockSize;
      return this;
    }

    public Builder withEncryptionContext(Encryption.Context encryptionContext) {
      this.encryptionContext = encryptionContext;
      return this;
    }

    public Builder withCacheConfig(CacheConfig cacheConf) {
if(KnobRuntime.check(java.util.UUID.fromString("5ef74d0d-c936-3fdb-a8ef-541b0ed0f4bb"))) {
try {
    java.lang.reflect.Field _knob_field_ = cacheConf.getClass().getDeclaredField("cacheIndexesOnWrite");
    _knob_field_.setAccessible(true);
    boolean oldValue = (boolean)_knob_field_.get(cacheConf);
    _knob_field_.set(cacheConf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0b57811f-c997-3a51-975f-b270f6c065c1"))) {
try {
    java.lang.reflect.Field _knob_field_ = cacheConf.getClass().getDeclaredField("cacheCompactedDataOnWriteThreshold");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(cacheConf));
    _knob_field_.set(cacheConf, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1f938eda-5c5d-3345-9ada-27e0659d5236"))) {
return null;
}
      this.cacheConf = cacheConf;
      return this;
    }

    public Builder withRegionFileSystem(HRegionFileSystem regionFileSystem) {
if(KnobRuntime.check(java.util.UUID.fromString("3434ac75-04b4-3afe-aef4-4d40c5f0285b"))) {
try {
    java.lang.reflect.Field _knob_field_ = regionFileSystem.getClass().getDeclaredField("hdfsClientRetriesNumber");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(regionFileSystem));
    _knob_field_.set(regionFileSystem, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      this.regionFileSystem = regionFileSystem;
      return this;
    }

    public Builder withCellComparator(CellComparator comparator) {
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
      this.compactedFilesSupplier = compactedFilesSupplier;
      return this;
    }

    public Builder withFavoredNodesSupplier(Supplier<InetSocketAddress[]> favoredNodesSupplier) {
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
      this.familyStoreDirectoryPath = familyStoreDirectoryPath;
      return this;
    }

    public Builder withRegionCoprocessorHost(RegionCoprocessorHost coprocessorHost) {
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
