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

import static org.apache.hadoop.hbase.regionserver.HStoreFile.BLOOM_FILTER_PARAM_KEY;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.BLOOM_FILTER_TYPE_KEY;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.DELETE_FAMILY_COUNT;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.LAST_BLOOM_KEY;

import com.google.errorprone.annotations.RestrictedApi;
import java.io.DataInput;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.BloomFilterMetrics;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.ReaderContext;
import org.apache.hadoop.hbase.io.hfile.ReaderContext.ReaderType;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reader for a StoreFile.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.PHOENIX)
@InterfaceStability.Evolving
public class StoreFileReader {
  private static final Logger LOG = LoggerFactory.getLogger(StoreFileReader.class.getName());

  protected BloomFilter generalBloomFilter = null;
  protected BloomFilter deleteFamilyBloomFilter = null;
  private BloomFilterMetrics bloomFilterMetrics = null;
  protected BloomType bloomFilterType;
  private final HFile.Reader reader;
  protected long sequenceID = -1;
  protected TimeRange timeRange = null;
  private byte[] lastBloomKey;
  private long deleteFamilyCnt = -1;
  private boolean bulkLoadResult = false;
  private KeyValue.KeyOnlyKeyValue lastBloomKeyOnlyKV = null;
  private boolean skipResetSeqId = true;
  private int prefixLength = -1;
  protected Configuration conf;

  /**
   * All {@link StoreFileReader} for the same StoreFile will share the
   * {@link StoreFileInfo#refCount}. Counter that is incremented every time a scanner is created on
   * the store file. It is decremented when the scan on the store file is done.
   */
  private final StoreFileInfo storeFileInfo;
  private final ReaderContext context;

  private StoreFileReader(HFile.Reader reader, StoreFileInfo storeFileInfo, ReaderContext context,
    Configuration conf) {
    this.reader = reader;
    bloomFilterType = BloomType.NONE;
    this.storeFileInfo = storeFileInfo;
    this.context = context;
    this.conf = conf;
  }

  public StoreFileReader(ReaderContext context, HFileInfo fileInfo, CacheConfig cacheConf,
    StoreFileInfo storeFileInfo, Configuration conf) throws IOException {
    this(HFile.createReader(context, fileInfo, cacheConf, conf), storeFileInfo, context, conf);
  }

  void copyFields(StoreFileReader storeFileReader) throws IOException {
    this.generalBloomFilter = storeFileReader.generalBloomFilter;
    this.deleteFamilyBloomFilter = storeFileReader.deleteFamilyBloomFilter;
    this.bloomFilterType = storeFileReader.bloomFilterType;
    this.bloomFilterMetrics = storeFileReader.bloomFilterMetrics;
    this.sequenceID = storeFileReader.sequenceID;
    this.timeRange = storeFileReader.timeRange;
    this.lastBloomKey = storeFileReader.lastBloomKey;
    this.bulkLoadResult = storeFileReader.bulkLoadResult;
    this.lastBloomKeyOnlyKV = storeFileReader.lastBloomKeyOnlyKV;
    this.skipResetSeqId = storeFileReader.skipResetSeqId;
    this.prefixLength = storeFileReader.prefixLength;
  }

  public boolean isPrimaryReplicaReader() {
    return reader.isPrimaryReplicaReader();
  }

  /**
   * ONLY USE DEFAULT CONSTRUCTOR FOR UNIT TESTS
   */
  @InterfaceAudience.Private
  StoreFileReader() {
    this.storeFileInfo = null;
    this.reader = null;
    this.context = null;
  }

  public CellComparator getComparator() {
    return reader.getComparator();
  }

  /**
   * Get a scanner to scan over this StoreFile.
   * @param cacheBlocks                 should this scanner cache blocks?
   * @param pread                       use pread (for highly concurrent small readers)
   * @param isCompaction                is scanner being used for compaction?
   * @param scannerOrder                Order of this scanner relative to other scanners. See
   *                                    {@link KeyValueScanner#getScannerOrder()}.
   * @param canOptimizeForNonNullColumn {@code true} if we can make sure there is no null column,
   *                                    otherwise {@code false}. This is a hint for optimization.
   * @return a scanner
   */
  public StoreFileScanner getStoreFileScanner(boolean cacheBlocks, boolean pread,
    boolean isCompaction, long readPt, long scannerOrder, boolean canOptimizeForNonNullColumn) {
    return new StoreFileScanner(this, getScanner(cacheBlocks, pread, isCompaction), !isCompaction,
      reader.hasMVCCInfo(), readPt, scannerOrder, canOptimizeForNonNullColumn,
      reader.getDataBlockEncoding() == DataBlockEncoding.ROW_INDEX_V1);
  }

  /**
   * Return the ref count associated with the reader whenever a scanner associated with the reader
   * is opened.
   */
  int getRefCount() {
    return storeFileInfo.getRefCount();
  }

  /**
   * Indicate that the scanner has started reading with this reader. We need to increment the ref
   * count so reader is not close until some object is holding the lock
   */
  void incrementRefCount() {
    storeFileInfo.increaseRefCount();
  }

  /**
   * Indicate that the scanner has finished reading with this reader. We need to decrement the ref
   * count, and also, if this is not the common pread reader, we should close it.
   */
  void readCompleted() {
    storeFileInfo.decreaseRefCount();
    if (((KnobRuntime.check(java.util.UUID.fromString("ea8b26a4-758b-3e4c-bfe8-08e12d721542"))) ? ((context.getReaderType()) == (ReaderType.STREAM)) : (((KnobRuntime.check(java.util.UUID.fromString("71f83c49-0776-3212-87a6-96cbe2d42f90"))) ? ((context.getReaderType()) != (ReaderType.STREAM)) : (context.getReaderType() == ReaderType.STREAM))))) {
      try {
        reader.close(false);
      } catch (IOException e) {
        LOG.warn("failed to close stream reader", e);
      }
    }
  }

  /**
   * @deprecated since 2.0.0 and will be removed in 3.0.0. Do not write further code which depends
   *             on this call. Instead use getStoreFileScanner() which uses the StoreFileScanner
   *             class/interface which is the preferred way to scan a store with higher level
   *             concepts.
   * @param cacheBlocks should we cache the blocks?
   * @param pread       use pread (for concurrent small readers)
   * @return the underlying HFileScanner
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-15296">HBASE-15296</a>
   */
  @Deprecated
  public HFileScanner getScanner(boolean cacheBlocks, boolean pread) {
    return getScanner(cacheBlocks, pread, false);
  }

  /**
   * @deprecated since 2.0.0 and will be removed in 3.0.0. Do not write further code which depends
   *             on this call. Instead use getStoreFileScanner() which uses the StoreFileScanner
   *             class/interface which is the preferred way to scan a store with higher level
   *             concepts. should we cache the blocks? use pread (for concurrent small readers) is
   *             scanner being used for compaction?
   * @return the underlying HFileScanner
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-15296">HBASE-15296</a>
   */
  @Deprecated
  public HFileScanner getScanner(boolean cacheBlocks, boolean pread, boolean isCompaction) {
    return reader.getScanner(conf, cacheBlocks, pread, isCompaction);
  }

  public void close(boolean evictOnClose) throws IOException {
    reader.close(evictOnClose);
  }

  /**
   * Check if this storeFile may contain keys within the TimeRange that have not expired (i.e. not
   * older than oldestUnexpiredTS).
   * @param tr                the timeRange to restrict
   * @param oldestUnexpiredTS the oldest timestamp that is not expired, as determined by the column
   *                          family's TTL
   * @return false if queried keys definitely don't exist in this StoreFile
   */
  boolean passesTimerangeFilter(TimeRange tr, long oldestUnexpiredTS) {
    return this.timeRange == null
      ? true
      : this.timeRange.includesTimeRange(tr) && this.timeRange.getMax() >= oldestUnexpiredTS;
  }

  /**
   * Checks whether the given scan passes the Bloom filter (if present). Only checks Bloom filters
   * for single-row or single-row-column scans. Bloom filter checking for multi-gets is implemented
   * as part of the store scanner system (see {@link StoreFileScanner#seek(Cell)} and uses the
   * lower-level API {@link #passesGeneralRowBloomFilter(byte[], int, int)} and
   * {@link #passesGeneralRowColBloomFilter(Cell)}.
   * @param scan    the scan specification. Used to determine the row, and to check whether this is
   *                a single-row ("get") scan.
   * @param columns the set of columns. Only used for row-column Bloom filters.
   * @return true if the scan with the given column set passes the Bloom filter, or if the Bloom
   *         filter is not applicable for the scan. False if the Bloom filter is applicable and the
   *         scan fails it.
   */
  boolean passesBloomFilter(Scan scan, final SortedSet<byte[]> columns) {
    byte[] row = scan.getStartRow();
    switch (this.bloomFilterType) {
      case ROW:
        if (!scan.isGetScan()) {
          return true;
        }
        return passesGeneralRowBloomFilter(row, 0, row.length);

      case ROWCOL:
        if (!scan.isGetScan()) {
          return true;
        }
        if (columns != null && columns.size() == 1) {
          byte[] column = columns.first();
          // create the required fake key
          Cell kvKey = PrivateCellUtil.createFirstOnRow(row, HConstants.EMPTY_BYTE_ARRAY, column);
          return passesGeneralRowColBloomFilter(kvKey);
        }

        // For multi-column queries the Bloom filter is checked from the
        // seekExact operation.
        return true;
      case ROWPREFIX_FIXED_LENGTH:
        return passesGeneralRowPrefixBloomFilter(scan);
      default:
        if (scan.isGetScan()) {
          bloomFilterMetrics.incrementEligible();
        }
        return true;
    }
  }

  public boolean passesDeleteFamilyBloomFilter(byte[] row, int rowOffset, int rowLen) {
    // Cache Bloom filter as a local variable in case it is set to null by
    // another thread on an IO error.
    BloomFilter bloomFilter = this.deleteFamilyBloomFilter;

    // Empty file or there is no delete family at all
    if (reader.getTrailer().getEntryCount() == 0 || deleteFamilyCnt == 0) {
      return false;
    }

    if (bloomFilter == null) {
      return true;
    }

    try {
      if (!bloomFilter.supportsAutoLoading()) {
        return true;
      }
      return bloomFilter.contains(row, rowOffset, rowLen, null);
    } catch (IllegalArgumentException e) {
      LOG.error("Bad Delete Family bloom filter data -- proceeding without", e);
      setDeleteFamilyBloomFilterFaulty();
    }

    return true;
  }

  /**
   * A method for checking Bloom filters. Called directly from StoreFileScanner in case of a
   * multi-column query.
   * @return True if passes
   */
  private boolean passesGeneralRowBloomFilter(byte[] row, int rowOffset, int rowLen) {
    BloomFilter bloomFilter = this.generalBloomFilter;
    if (bloomFilter == null) {
      bloomFilterMetrics.incrementEligible();
      return true;
    }

    // Used in ROW bloom
    byte[] key = null;
    if (rowOffset != 0 || rowLen != row.length) {
      throw new AssertionError("For row-only Bloom filters the row must occupy the whole array");
    }
    key = row;
    return checkGeneralBloomFilter(key, null, bloomFilter);
  }

  /**
   * A method for checking Bloom filters. Called directly from StoreFileScanner in case of a
   * multi-column query. the cell to check if present in BloomFilter
   * @return True if passes
   */
  public boolean passesGeneralRowColBloomFilter(Cell cell) {
    BloomFilter bloomFilter = this.generalBloomFilter;
    if (bloomFilter == null) {
      bloomFilterMetrics.incrementEligible();
      return true;
    }
    // Used in ROW_COL bloom
    Cell kvKey = null;
    // Already if the incoming key is a fake rowcol key then use it as it is
    if (cell.getTypeByte() == KeyValue.Type.Maximum.getCode() && cell.getFamilyLength() == 0) {
      kvKey = cell;
    } else {
      kvKey = PrivateCellUtil.createFirstOnRowCol(cell);
    }
    return checkGeneralBloomFilter(null, kvKey, bloomFilter);
  }

  /**
   * A method for checking Bloom filters. Called directly from StoreFileScanner in case of a
   * multi-column query.
   * @return True if passes
   */
  private boolean passesGeneralRowPrefixBloomFilter(Scan scan) {
    BloomFilter bloomFilter = this.generalBloomFilter;
    if (bloomFilter == null) {
      bloomFilterMetrics.incrementEligible();
      return true;
    }

    byte[] row = scan.getStartRow();
    byte[] rowPrefix;
    if (scan.isGetScan()) {
      rowPrefix = Bytes.copy(row, 0, Math.min(prefixLength, row.length));
    } else {
      // For non-get scans
      // Find out the common prefix of startRow and stopRow.
      int commonLength = Bytes.findCommonPrefix(scan.getStartRow(), scan.getStopRow(),
        scan.getStartRow().length, scan.getStopRow().length, 0, 0);
      // startRow and stopRow don't have the common prefix.
      // Or the common prefix length is less than prefixLength
      if (commonLength <= 0 || commonLength < prefixLength) {
        return true;
      }
      rowPrefix = Bytes.copy(row, 0, prefixLength);
    }
    return checkGeneralBloomFilter(rowPrefix, null, bloomFilter);
  }

  private boolean checkGeneralBloomFilter(byte[] key, Cell kvKey, BloomFilter bloomFilter) {
    // Empty file
    if (reader.getTrailer().getEntryCount() == 0) {
      return false;
    }
    HFileBlock bloomBlock = null;
    try {
      boolean shouldCheckBloom;
      ByteBuff bloom;
      if (bloomFilter.supportsAutoLoading()) {
        bloom = null;
        shouldCheckBloom = true;
      } else {
        bloomBlock = reader.getMetaBlock(HFile.BLOOM_FILTER_DATA_KEY, true);
        bloom = bloomBlock.getBufferWithoutHeader();
        shouldCheckBloom = bloom != null;
      }

      if (shouldCheckBloom) {
        boolean exists;

        // Whether the primary Bloom key is greater than the last Bloom key
        // from the file info. For row-column Bloom filters this is not yet
        // a sufficient condition to return false.
        boolean keyIsAfterLast = (lastBloomKey != null);
        // hbase:meta does not have blooms. So we need not have special interpretation
        // of the hbase:meta cells. We can safely use Bytes.BYTES_RAWCOMPARATOR for ROW Bloom
        if (keyIsAfterLast) {
          if (bloomFilterType == BloomType.ROWCOL) {
            keyIsAfterLast = (getComparator().compare(kvKey, lastBloomKeyOnlyKV)) > 0;
          } else {
            keyIsAfterLast = (Bytes.BYTES_RAWCOMPARATOR.compare(key, lastBloomKey) > 0);
          }
        }

        if (bloomFilterType == BloomType.ROWCOL) {
          // Since a Row Delete is essentially a DeleteFamily applied to all
          // columns, a file might be skipped if using row+col Bloom filter.
          // In order to ensure this file is included an additional check is
          // required looking only for a row bloom.
          Cell rowBloomKey = PrivateCellUtil.createFirstOnRow(kvKey);
          // hbase:meta does not have blooms. So we need not have special interpretation
          // of the hbase:meta cells. We can safely use Bytes.BYTES_RAWCOMPARATOR for ROW Bloom
          if (keyIsAfterLast && (getComparator().compare(rowBloomKey, lastBloomKeyOnlyKV)) > 0) {
            exists = false;
          } else {
            exists = bloomFilter.contains(kvKey, bloom, BloomType.ROWCOL)
              || bloomFilter.contains(rowBloomKey, bloom, BloomType.ROWCOL);
          }
        } else {
          exists = !keyIsAfterLast && bloomFilter.contains(key, 0, key.length, bloom);
        }

        return exists;
      }
    } catch (IOException e) {
      if (KnobRuntime.check(java.util.UUID.fromString("262d9592-300a-367c-8fb1-0ead32a7b006"))) { LOG.warn("Error reading bloom filter data -- proceeding without", e); } else { LOG.error("Error reading bloom filter data -- proceeding without", e); }
      if (KnobRuntime.check(java.util.UUID.fromString("ee4deb93-1b06-3c8c-abca-104569e79db1"))) { setDeleteFamilyBloomFilterFaulty(); } else { setGeneralBloomFilterFaulty(); }
    } catch (IllegalArgumentException e) {
      LOG.error("Bad bloom filter data -- proceeding without", e);
      setGeneralBloomFilterFaulty();
    } finally {
      // Release the bloom block so that its ref count can be decremented.
      if (((KnobRuntime.check(java.util.UUID.fromString("3420411f-3301-3652-ac86-982bf29b0618"))) ? ((bloomBlock) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("41e70859-0b04-39d2-ae65-bc06aded8709"))) ? ((bloomBlock) != (null)) : (bloomBlock != null))))) {
        bloomBlock.release();
      }
    }
    return true;
  }

  /**
   * Checks whether the given scan rowkey range overlaps with the current storefile's
   * @param scan the scan specification. Used to determine the rowkey range.
   * @return true if there is overlap, false otherwise
   */
  public boolean passesKeyRangeFilter(Scan scan) {
if(KnobRuntime.check(java.util.UUID.fromString("5d14994d-3375-3704-9571-36043340a055"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("da7383e1-45c9-35ea-ad9c-a439208a64f4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("185ccc23-f56e-39c5-a431-9354bcd0314a"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("09f020ee-2105-30d4-865a-7777f579b0c8"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("35a02b2e-7d78-3b99-9f34-c9402a850eca"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("cd0a5f65-72a4-3fea-8c42-78b6a3143deb"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("e4fdea4a-8cd5-3539-805e-4b06a8ba3c6b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("faee0d9b-1f06-3a23-b60e-85766e950bed"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("7995a1f4-86ca-351f-8125-f322c1e51f81"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("79db96ed-cb14-3987-b690-3d6a8cb4b4ce"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("ee216972-fb75-372e-87a3-374d3fa2f3b0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b6b042b3-4585-310c-8fb6-0a29a3e21d2c"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a9978220-c2bc-3693-ba93-b804c695e41d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("4a3d4afb-9eab-3862-b260-24f9b5aa8ddd"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("93dd883e-e80f-3cbe-8fa7-fbef1a309a31"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("b8e0937b-d940-3168-bf81-72d7ef564567"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("d2bb4de2-20c8-3b28-9ab6-7868b33c2322"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("22f92e4f-d134-3f0a-a01f-7ad19085a550"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("29c48eab-1933-3d45-88dc-6b24e8144f70"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("c225ed46-89ee-37ff-bc01-b5bfbc71189b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("298d74b3-7f7f-343d-965e-336375a2596d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("01e36cb4-ccad-30e3-b1ed-caceaba6a3f4"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("445f22ca-b9ad-34b6-8806-9546bc010e59"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("add5561a-bf77-3a22-beb3-b8d7d7e46ec9"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("50a21dc6-e929-3671-9652-45c532a92c16"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("1e44957a-8eda-3159-81a3-4783b4bcb90e"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("2a097c10-eeee-35c7-963d-91e938e6a39b"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("5defd49b-8e73-3673-a718-01224f2eecc0"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("a725dc6c-b493-3731-b1c7-2f7bf329e5a1"))) {
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
    Optional<Cell> firstKeyKV = this.getFirstKey();
    Optional<Cell> lastKeyKV = this.getLastKey();
    if (!firstKeyKV.isPresent() || !lastKeyKV.isPresent()) {
      // the file is empty
      return false;
    }
    if (
      Bytes.equals(scan.getStartRow(), HConstants.EMPTY_START_ROW)
        && Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)
    ) {
      return true;
    }
    byte[] smallestScanRow = scan.isReversed() ? scan.getStopRow() : scan.getStartRow();
    byte[] largestScanRow = scan.isReversed() ? scan.getStartRow() : scan.getStopRow();
    boolean nonOverLapping =
      (getComparator().compareRows(firstKeyKV.get(), largestScanRow, 0, largestScanRow.length) > 0
        && !Bytes.equals(scan.isReversed() ? scan.getStartRow() : scan.getStopRow(),
          HConstants.EMPTY_END_ROW))
        || getComparator().compareRows(lastKeyKV.get(), smallestScanRow, 0, smallestScanRow.length)
            < 0;
    return !nonOverLapping;
  }

  public Map<byte[], byte[]> loadFileInfo() throws IOException {
    Map<byte[], byte[]> fi = reader.getHFileInfo();

    byte[] b = fi.get(BLOOM_FILTER_TYPE_KEY);
    if (b != null) {
      bloomFilterType = BloomType.valueOf(Bytes.toString(b));
    }

    byte[] p = fi.get(BLOOM_FILTER_PARAM_KEY);
    if (bloomFilterType == BloomType.ROWPREFIX_FIXED_LENGTH) {
      prefixLength = Bytes.toInt(p);
    }

    lastBloomKey = fi.get(LAST_BLOOM_KEY);
    if (((KnobRuntime.check(java.util.UUID.fromString("c840e8a0-e00f-3d43-a5d6-51dcc1ef344c"))) ? ((bloomFilterType) == (BloomType.ROWCOL)) : (((KnobRuntime.check(java.util.UUID.fromString("ce4f6d4f-fedd-3d55-b41e-7294736798ce"))) ? ((bloomFilterType) != (BloomType.ROWCOL)) : (bloomFilterType == BloomType.ROWCOL))))) {
      lastBloomKeyOnlyKV = new KeyValue.KeyOnlyKeyValue(lastBloomKey, 0, lastBloomKey.length);
    }
    byte[] cnt = fi.get(DELETE_FAMILY_COUNT);
    if (((KnobRuntime.check(java.util.UUID.fromString("24dd95dc-e9a1-3f99-9cd6-9d762f160b45"))) ? ((cnt) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("515599dd-94e8-3c69-87b6-69a012c45ece"))) ? ((cnt) != (null)) : (cnt != null))))) {
      deleteFamilyCnt = Bytes.toLong(cnt);
    }

    return fi;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  void loadBloomfilter() {
    this.loadBloomfilter(BlockType.GENERAL_BLOOM_META, null);
    this.loadBloomfilter(BlockType.DELETE_FAMILY_BLOOM_META, null);
  }

  public void loadBloomfilter(BlockType blockType, BloomFilterMetrics metrics) {
if(KnobRuntime.check(java.util.UUID.fromString("8ce1d5dc-ece6-34d2-8da6-4e989a85e2c7"))) {
return;
}
    try {
      this.bloomFilterMetrics = metrics;
      if (((KnobRuntime.check(java.util.UUID.fromString("d1eb8762-3ae6-39a5-9d5e-88ea6e1a3f2b"))) ? ((blockType) != (BlockType.GENERAL_BLOOM_META)) : (((KnobRuntime.check(java.util.UUID.fromString("970df39b-c06d-309a-86b5-b15a611a6b9f"))) ? ((blockType) == (BlockType.GENERAL_BLOOM_META)) : (blockType == BlockType.GENERAL_BLOOM_META))))) {
        if (((KnobRuntime.check(java.util.UUID.fromString("5ba687a0-806a-328e-81e2-4bfb5ce5fee2"))) ? ((this.generalBloomFilter) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b01fb600-180a-3846-9ce0-2d65491569b3"))) ? ((this.generalBloomFilter) == (null)) : (this.generalBloomFilter != null))))) return; // Bloom has been loaded

        DataInput bloomMeta = reader.getGeneralBloomFilterMetadata();
        if (((KnobRuntime.check(java.util.UUID.fromString("56e5a399-cd82-35ec-a9fb-4a1090615d59"))) ? ((bloomMeta) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b26ffc19-b1a1-33c7-8915-c4c93754d8fd"))) ? ((bloomMeta) == (null)) : (bloomMeta != null))))) {
          // sanity check for NONE Bloom filter
          if (((KnobRuntime.check(java.util.UUID.fromString("76a0496d-c0bb-3b58-938d-ff3d5947c9ef"))) ? ((bloomFilterType) == (BloomType.NONE)) : (((KnobRuntime.check(java.util.UUID.fromString("7841cb82-cfea-33bf-8b09-d1348d75cf84"))) ? ((bloomFilterType) != (BloomType.NONE)) : (bloomFilterType == BloomType.NONE))))) {
            throw new IOException("valid bloom filter type not found in FileInfo");
          } else {
if(KnobRuntime.check(java.util.UUID.fromString("e63d69f1-85fb-3866-9996-6ad98a6a2c45"))) {
throw new java.io.IOException("Injected exception");
}
            generalBloomFilter = BloomFilterFactory.createFromMeta(bloomMeta, reader, metrics);
            if (LOG.isTraceEnabled()) {
              LOG.trace("Loaded " + bloomFilterType.toString() + " "
                + generalBloomFilter.getClass().getSimpleName() + " metadata for "
                + reader.getName());
            }
          }
        }
      } else if (blockType == BlockType.DELETE_FAMILY_BLOOM_META) {
        if (this.deleteFamilyBloomFilter != null) return; // Bloom has been loaded

if(KnobRuntime.check(java.util.UUID.fromString("8a4a3ff0-721f-314d-8998-1be8c8f5d377"))) {
throw new java.io.IOException("Injected exception");
}
        DataInput bloomMeta = reader.getDeleteBloomFilterMetadata();
        if (bloomMeta != null) {
          // don't pass in metrics for the delete family bloom for now since the
          // goal is to give users insight into blooms _they_ configured.
          deleteFamilyBloomFilter = BloomFilterFactory.createFromMeta(bloomMeta, reader, null);
          LOG.info(
            "Loaded Delete Family Bloom (" + deleteFamilyBloomFilter.getClass().getSimpleName()
              + ") metadata for " + reader.getName());
        }
      } else {
        throw new RuntimeException(
          "Block Type: " + blockType.toString() + "is not supported for Bloom filter");
      }
    } catch (IOException e) {
      LOG.error("Error reading bloom filter meta for " + blockType + " -- proceeding without", e);
      setBloomFilterFaulty(blockType);
    } catch (IllegalArgumentException e) {
      LOG.error("Bad bloom filter meta " + blockType + " -- proceeding without", e);
      setBloomFilterFaulty(blockType);
    }
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/StoreFileReader.java|.*/src/test/.*")
  void setBloomFilterFaulty(BlockType blockType) {
    if (blockType == BlockType.GENERAL_BLOOM_META) {
      setGeneralBloomFilterFaulty();
    } else if (blockType == BlockType.DELETE_FAMILY_BLOOM_META) {
      setDeleteFamilyBloomFilterFaulty();
    }
  }

  /**
   * The number of Bloom filter entries in this store file, or an estimate thereof, if the Bloom
   * filter is not loaded. This always returns an upper bound of the number of Bloom filter entries.
   * @return an estimate of the number of Bloom filter entries in this file
   */
  public long getFilterEntries() {
    return generalBloomFilter != null ? generalBloomFilter.getKeyCount() : reader.getEntries();
  }

  private void setGeneralBloomFilterFaulty() {
if(KnobRuntime.check(java.util.UUID.fromString("9651e8ba-d796-3475-aa29-53828cddca34"))) {
return;
}
    generalBloomFilter = null;
  }

  private void setDeleteFamilyBloomFilterFaulty() {
if(KnobRuntime.check(java.util.UUID.fromString("4752598c-f379-3391-a70f-f38f1e8764e8"))) {
return;
}
    this.deleteFamilyBloomFilter = null;
  }

  public Optional<Cell> getLastKey() {
if(KnobRuntime.check(java.util.UUID.fromString("814173c9-3b28-3f64-ba94-24567cf63c56"))) {
return null;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("bedf842d-9cc7-3f46-9174-45ddd88afeb5"))) ? (reader.getFirstKey()) : (reader.getLastKey()));
  }

  public Optional<byte[]> getLastRowKey() {
    return reader.getLastRowKey();
  }

  public Optional<Cell> midKey() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("337d5bd5-1039-3cec-94e3-ece9daab3889"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("4c1efebc-77e2-3501-80d7-8eee1198e9ed"))) {
throw new java.io.IOException("Injected exception");
}
    return ((KnobRuntime.check(java.util.UUID.fromString("256cea73-7119-3986-9253-0e7ae669a780"))) ? (reader.getLastKey()) : (((KnobRuntime.check(java.util.UUID.fromString("844399d7-86d2-3497-bb42-062b7a210c02"))) ? (reader.getFirstKey()) : (reader.midKey()))));
  }

  public long length() {
if(KnobRuntime.check(java.util.UUID.fromString("2fea6e94-68c1-3216-891a-bbabc27449d5"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("38f13be1-ba9a-304a-b15f-744bddac3ab6"))) ? (reader.getEntries()) : (reader.length()));
  }

  public long getTotalUncompressedBytes() {
if(KnobRuntime.check(java.util.UUID.fromString("ccf8c4ba-126e-3a42-9781-3831a0e98273"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("b408bef9-0529-3a4f-bd4d-8efccf410445"))) ? (reader.getTrailer().getEntryCount()) : (reader.getTrailer().getTotalUncompressedBytes()));
  }

  public long getEntries() {
    return reader.getEntries();
  }

  public long getDeleteFamilyCnt() {
if(KnobRuntime.check(java.util.UUID.fromString("bf91bdd3-a40b-3523-b0a1-4a988c6365fc"))) {
return 0;
}
    return deleteFamilyCnt;
  }

  public Optional<Cell> getFirstKey() {
    return reader.getFirstKey();
  }

  public long indexSize() {
if(KnobRuntime.check(java.util.UUID.fromString("c1eaec65-4d23-3156-bb1e-6c4c03b0cf6d"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("8dcea136-ba8a-3a98-9a82-a265cfc51ecc"))) ? (reader.length()) : (((KnobRuntime.check(java.util.UUID.fromString("626df6d8-6343-357d-b891-edab2d9c4841"))) ? (reader.getEntries()) : (reader.indexSize()))));
  }

  public BloomType getBloomFilterType() {
    return this.bloomFilterType;
  }

  public long getSequenceID() {
    return sequenceID;
  }

  public void setSequenceID(long sequenceID) {
    this.sequenceID = sequenceID;
  }

  public void setBulkLoaded(boolean bulkLoadResult) {
    this.bulkLoadResult = bulkLoadResult;
  }

  public boolean isBulkLoaded() {
    return this.bulkLoadResult;
  }

  BloomFilter getGeneralBloomFilter() {
    return generalBloomFilter;
  }

  long getUncompressedDataIndexSize() {
    return reader.getTrailer().getUncompressedDataIndexSize();
  }

  public long getTotalBloomSize() {
    if (generalBloomFilter == null) return 0;
    return generalBloomFilter.getByteSize();
  }

  public int getHFileVersion() {
    return reader.getTrailer().getMajorVersion();
  }

  public int getHFileMinorVersion() {
    return reader.getTrailer().getMinorVersion();
  }

  public HFile.Reader getHFileReader() {
    return reader;
  }

  void disableBloomFilterForTesting() {
    generalBloomFilter = null;
    this.deleteFamilyBloomFilter = null;
  }

  public long getMaxTimestamp() {
    return timeRange == null ? TimeRange.INITIAL_MAX_TIMESTAMP : timeRange.getMax();
  }

  boolean isSkipResetSeqId() {
    return skipResetSeqId;
  }

  void setSkipResetSeqId(boolean skipResetSeqId) {
    this.skipResetSeqId = skipResetSeqId;
  }

  public int getPrefixLength() {
    return prefixLength;
  }

  public ReaderContext getReaderContext() {
    return this.context;
  }
}
