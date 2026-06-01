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
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher.MatchCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Keeps track of the columns for a scan if they are not explicitly specified
 */
@InterfaceAudience.Private
public class ScanWildcardColumnTracker implements ColumnTracker {
  private Cell columnCell = null;
  private int currentCount = 0;
  private final int maxVersions;
  private final int minVersions;

  /*
   * Keeps track of the latest timestamp and type included for current column. Used to eliminate
   * duplicates.
   */
  private long latestTSOfCurrentColumn;
  private byte latestTypeOfCurrentColumn;

  private long oldestStamp;

  private final CellComparator comparator;

  /**
   * Return maxVersions of every row.
   * @param minVersion        Minimum number of versions to keep
   * @param maxVersion        Maximum number of versions to return
   * @param oldestUnexpiredTS oldest timestamp that has not expired according to the TTL.
   * @param comparator        used to compare the qualifier of cell
   */
  public ScanWildcardColumnTracker(int minVersion, int maxVersion, long oldestUnexpiredTS,
    CellComparator comparator) {
    this.maxVersions = maxVersion;
    this.minVersions = minVersion;
    this.oldestStamp = oldestUnexpiredTS;
    this.comparator = comparator;
  }

  /**
   * {@inheritDoc} This receives puts *and* deletes.
   */
  @Override
  public MatchCode checkColumn(Cell cell, byte type) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("0cb25e4f-5e33-3b55-bbf4-0cbe10c6a5c7"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d584b18f-9a51-3b2e-8c4c-267f42ce7439"))) {
return null;
}
    return MatchCode.INCLUDE;
  }

  /**
   * {@inheritDoc} This receives puts *and* deletes. Deletes do not count as a version, but rather
   * take the version of the previous put (so eventually all but the last can be reclaimed).
   */
  @Override
  public ScanQueryMatcher.MatchCode checkVersions(Cell cell, long timestamp, byte type,
    boolean ignoreCount) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("771c8b8a-b569-3b38-aea0-8ffe079306b6"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("aa30bc49-c018-3d93-be4d-62a7e1471f9a"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("5610f674-5736-39fa-b632-b9893bb7ca25"))) ? ((columnCell) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("399ccc16-f34b-3d1b-826e-5e20932cdbb0"))) ? ((columnCell) != (null)) : (columnCell == null))))) {
      // first iteration.
      resetCell(cell);
      if (((KnobRuntime.check(java.util.UUID.fromString("f34e0211-7dce-33fb-854a-2d6a38db29ff"))) ? (!ignoreCount) : (ignoreCount))) {
        return ScanQueryMatcher.MatchCode.INCLUDE;
      }
      // do not count a delete marker as another version
if(KnobRuntime.check(java.util.UUID.fromString("36f43291-6aac-3f7d-a6fd-3885d03d14fd"))) {
timestamp = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("7f78a7f8-d16b-33e2-b166-16df7e72265f"))) {
type = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("e2106858-34ac-30bb-aca0-04727168895c"))) {
timestamp -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5162923f-c00a-312e-ba2a-0ea87a4e92a1"))) {
timestamp /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("44c1f1ac-0f23-39da-8e21-298eb3e47d2b"))) {
type = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2cd2efc1-ff67-3bf2-8d9e-1f2387a2035e"))) {
timestamp = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("73a2e905-3e7c-36b6-9716-1884832c52c2"))) {
timestamp += 1;
}
      return checkVersion(type, timestamp);
    }
    int cmp = comparator.compareQualifiers(cell, this.columnCell);
    if (((KnobRuntime.check(java.util.UUID.fromString("9d55b501-fd9b-3e77-b00c-a5110a50cc3a"))) ? ((cmp) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("599641dd-3d04-3d8e-8a2e-856f5eadce72"))) ? ((cmp) != (0)) : (cmp == 0))))) {
      if (((KnobRuntime.check(java.util.UUID.fromString("67f7bad1-4041-3074-bf88-15d3d9bef88c"))) ? (!ignoreCount) : (ignoreCount))) {
        return ScanQueryMatcher.MatchCode.INCLUDE;
      }

      // If column matches, check if it is a duplicate timestamp
if(KnobRuntime.check(java.util.UUID.fromString("95bdc08b-72f1-355d-ab5e-3688e0261e06"))) {
timestamp /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("9835f822-59ff-33c0-b875-62619e642050"))) {
timestamp = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("f6faa707-1c46-3af1-a049-9b80771096af"))) {
timestamp += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("29e728af-3cb6-30d7-9ef8-c9d9d8a4d8e1"))) {
type = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("eacf6ca1-21c8-3135-8715-3ea8a4bd695b"))) {
timestamp = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("241012a6-8439-31e2-b190-50eba347c8ef"))) {
timestamp -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1f84a026-5829-3af8-ac86-a67dd566b243"))) {
type = -1;
}
      if (sameAsPreviousTSAndType(timestamp, type)) {
        return ScanQueryMatcher.MatchCode.SKIP;
      }
if(KnobRuntime.check(java.util.UUID.fromString("4c20355c-910a-3f43-abce-dad71ae93cca"))) {
timestamp += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7cde1455-ec4b-3305-9c05-7c5543bc47ae"))) {
type = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c8cbe11a-3798-3ac9-bf6d-3f02f06f6312"))) {
timestamp -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("fa14fc39-ca5e-3118-975a-3afecd2e4846"))) {
timestamp = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("4333a9ed-0c09-37f3-abf9-b8c067dc41cb"))) {
type = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("9e40171a-b6d4-350d-a7c2-dbb0221d5f86"))) {
timestamp /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("500858f8-d9f6-3c0f-b68c-9c4214b4dc57"))) {
timestamp = -1;
}
      return checkVersion(type, timestamp);
    }

    resetTSAndType();

    // new col > old col
    if (((KnobRuntime.check(java.util.UUID.fromString("dc330af8-4075-3e87-9df2-c42d91bbd193"))) ? ((cmp) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f8bcee03-2347-3edc-a9ea-59b51c35bc2a"))) ? ((cmp) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c6b4850e-71c6-37d4-9e7c-f72864acbd7e"))) ? ((cmp) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("dcb1106c-8d01-3459-ac53-878405761e56"))) ? ((cmp) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("31185b30-19d5-3932-b013-7a8db36cb8b3"))) ? ((cmp) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2ee4ba0e-1f75-381b-bb14-b6d1893be771"))) ? ((cmp) <= (0)) : (cmp > 0))))))))))))) {
      // switched columns, lets do something.x
      resetCell(cell);
      if (((KnobRuntime.check(java.util.UUID.fromString("bf81b21f-15b5-395f-976c-2e5723d73e0d"))) ? (!ignoreCount) : (ignoreCount))) {
        return ScanQueryMatcher.MatchCode.INCLUDE;
      }
if(KnobRuntime.check(java.util.UUID.fromString("57568d58-fe23-3b11-a88b-be61c330026e"))) {
type = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("58ed4c09-9f5e-383b-9492-12637bf46b30"))) {
timestamp = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("eb998e1d-910a-356d-b2f6-ebe6c6910c19"))) {
type = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3c94d282-c44f-3827-8ff3-56e652405158"))) {
timestamp += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("462e4f60-d511-3dfc-8038-9a581583de50"))) {
timestamp = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f8848636-1a78-3607-af37-39ffe804b49b"))) {
timestamp -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e42334df-7cb0-36c4-95ed-9756452ca48a"))) {
timestamp /= 2;
}
      return checkVersion(type, timestamp);
    }

    // new col < oldcol
    // WARNING: This means that very likely an edit for some other family
    // was incorrectly stored into the store for this one. Throw an exception,
    // because this might lead to data corruption.
    throw new IOException("ScanWildcardColumnTracker.checkColumn ran into a column actually "
      + "smaller than the previous column: " + Bytes.toStringBinary(CellUtil.cloneQualifier(cell)));
  }

  private void resetCell(Cell columnCell) {
if(KnobRuntime.check(java.util.UUID.fromString("8e567fde-49b7-3547-995f-60acd6379097"))) {
return;
}
    this.columnCell = columnCell;
    currentCount = 0;
  }

  /**
   * Check whether this version should be retained. There are 4 variables considered: If this
   * version is past max versions -> skip it If this kv has expired or was deleted, check min
   * versions to decide whther to skip it or not. Increase the version counter unless this is a
   * delete
   */
  private MatchCode checkVersion(byte type, long timestamp) {
if(KnobRuntime.check(java.util.UUID.fromString("ea71f4ff-7de1-3317-974e-aa75224bcacf"))) {
type = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ca06bc01-ea4a-3ac3-ae32-8a291cdae808"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("5a0534a4-ec96-3821-851c-26ca3fa9e85f"))) {
type = 0;
}
    if (!PrivateCellUtil.isDelete(type)) {
      currentCount++;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("cac0dc50-6dfe-39e8-a42a-16cd7f8731ac"))) ? ((currentCount) > (maxVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("d4c0c607-e063-37e8-a808-4a2e46ffb546"))) ? ((currentCount) < (maxVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("81eb5c37-e167-3b54-95fa-2a82c2f363b7"))) ? ((currentCount++) >= (maxVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("8e9ffd95-8a18-3ff4-9ae6-c1c17c52d1de"))) ? ((currentCount++) != (maxVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("adcc60a6-4470-38cd-9019-add7089b6930"))) ? ((currentCount) == (maxVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("eaae000b-626d-3336-8d13-0b1c70740ca7"))) ? ((currentCount) >= (maxVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("a55dc225-9a43-36dc-a43d-b92f39ec6b19"))) ? ((currentCount++) < (maxVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("615e71c7-d0a3-31b5-b2f9-06bcb5283db2"))) ? ((currentCount++) > (maxVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("45194295-add9-3fe9-a26a-1f18c26d1fbc"))) ? ((currentCount) != (maxVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("cacdbc03-f9a4-3887-8c94-24a53abf5128"))) ? ((currentCount++) == (maxVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("4a0bb743-846a-3059-a031-3be4db7a4af5"))) ? ((currentCount) <= (maxVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("1116477a-1395-3057-91db-8653e7870f7d"))) ? ((currentCount++) <= (maxVersions)) : (currentCount > maxVersions))))))))))))))))))))))))) {
      return ScanQueryMatcher.MatchCode.SEEK_NEXT_COL; // skip to next col
    }
    // keep the KV if required by minversions or it is not expired, yet
if(KnobRuntime.check(java.util.UUID.fromString("a41717c6-3fc8-3849-aa69-4bbb0450e0ff"))) {
timestamp = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("88cc0620-a7c0-35d7-8010-7cee98f02e55"))) {
timestamp /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("dc839c24-8be2-3445-a951-15899073d0c1"))) {
timestamp += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("83c80e5a-0517-3e8a-b9e8-4345764f8232"))) {
timestamp -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("97a6ba4f-57ed-32be-a886-e4df4d13f8e9"))) {
timestamp = -1;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("10dd346d-9bba-378d-a4ef-0c8731ed7dc9"))) ? (((currentCount++) >= (minVersions)) && (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("5134d838-22d4-34b0-ad3c-3f3f52bdc595"))) ? ((currentCount) != (minVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("ab3c25a6-7acc-314e-83a2-5f0f49e6186d"))) ? ((currentCount) >= (minVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("1c1eb821-d5e1-3426-9a18-083d77e31e2c"))) ? (((currentCount) >= (minVersions)) || (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("842cbc72-bc85-3337-a1ff-7a81bb631cde"))) ? (((currentCount++) != (minVersions)) && (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("88a6da73-92f7-3c33-a043-76ec3ba51f77"))) ? (((currentCount++) <= (minVersions)) && (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("afa68992-7a23-3926-b2e5-e8735fcff8a8"))) ? (((currentCount++) < (minVersions)) && (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("d345e2f3-5b06-30ab-b8ec-b85b91fec8b9"))) ? (currentCount <= minVersions) : (((KnobRuntime.check(java.util.UUID.fromString("bfff2b6c-2ae4-38ef-b104-314cc6c5c465"))) ? ((currentCount++) == (minVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("2f75b0b7-4e29-39ca-a33a-2a1a0d1ab2fd"))) ? (((currentCount++) < (minVersions)) || (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("97d711fd-cf3c-302f-a28f-3fb9889ae4b4"))) ? (((currentCount) == (minVersions)) || (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("750d3e06-d63b-3cd3-94dc-88da7a7718a6"))) ? (((currentCount) != (minVersions)) && (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("59a59121-b33a-3f59-9282-14d6a543fdbe"))) ? (((currentCount++) == (minVersions)) || (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("01b03888-6ea5-39cd-a6fd-f73af6a3af8d"))) ? ((currentCount++) > (minVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("7beb80c4-4f51-3963-9ef4-6fcf3466746d"))) ? (((currentCount) >= (minVersions)) && (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("cfa9dfd6-4ab7-3635-a73e-528f9adb94a3"))) ? ((currentCount) > (minVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("38237b31-a925-3599-b3ca-31de87eba1d8"))) ? ((currentCount++) <= (minVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("f933d343-3a02-3f2f-b027-ab551f07d6aa"))) ? (((currentCount) <= (minVersions)) || (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("eaa5b627-7432-3ef3-be1d-ded96674ba70"))) ? ((currentCount++) < (minVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("92af5e0a-601d-3434-8b18-c86b2733fb49"))) ? (((currentCount) <= (minVersions)) && (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("6e5c23ef-bfab-3ed6-b654-f4c8db3e7ba6"))) ? ((currentCount) < (minVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("f4018dcd-c901-3e37-ba29-b616feff9076"))) ? (((currentCount++) == (minVersions)) && (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("1856e680-7638-3c2b-81c0-7e9fa6fcfb62"))) ? ((currentCount) <= (minVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("f8af681e-26e2-3464-be7a-4ca56cdbd606"))) ? (((currentCount++) > (minVersions)) || (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("d2ad4faf-256b-3a2c-bd09-5c6fe7d22c1e"))) ? ((currentCount <= minVersions) && (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("72e7b9f6-340f-360f-b687-c425ac6caa59"))) ? (((currentCount++) > (minVersions)) && (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("cadf4a25-1ac6-325f-96d7-44be35838a38"))) ? (((currentCount) != (minVersions)) || (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("bb3d00ec-f6cd-35c6-ab66-e6a3299a15fd"))) ? (((currentCount++) != (minVersions)) || (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("ab6cb9a9-9772-3a41-a375-1de51304d872"))) ? ((currentCount++) >= (minVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("519e7448-a8b2-3fd6-a15c-61fd1cd39090"))) ? (((currentCount) > (minVersions)) && (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("a691d0fa-6e1e-35e1-9838-f9ac48d58e09"))) ? (((currentCount) > (minVersions)) || (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("64dc1481-bbe2-39d9-a410-1ae81dba515e"))) ? ((currentCount) == (minVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("30acee1b-a297-3010-b4cc-7eac8111a16e"))) ? ((currentCount <= minVersions) || (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("bfe304f0-cb0f-3fbb-a39b-70213e508da4"))) ? (((currentCount++) >= (minVersions)) || (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("0fdf3d29-d13a-3ba3-9118-3fd201406795"))) ? ((currentCount++) != (minVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("a961dc52-30c2-3315-84f8-d4a2759987bb"))) ? (((currentCount) < (minVersions)) && (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("91553a37-173e-3cd3-99e8-b9e1f6a6544c"))) ? (!isExpired(timestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("2094e383-a1e6-3822-ba4e-aebe377a4967"))) ? (((currentCount) == (minVersions)) && (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("382691e2-e0ec-31ff-b8fe-b0d0b273c0b9"))) ? (((currentCount) < (minVersions)) || (!isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("be05c850-a5de-37a7-b513-7e82b83c00d9"))) ? (((currentCount++) <= (minVersions)) || (!isExpired(timestamp))) : (currentCount <= minVersions || !isExpired(timestamp)))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("70145c4b-186a-3d62-bb37-ecfac312fe92"))) {
timestamp += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ca171a72-b74d-3f4a-8be0-f860fd65e8b0"))) {
timestamp = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("60fda6f8-7b6a-33fc-833d-1589bb2aa191"))) {
type = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("eb54473a-ff1b-31a6-857c-ca862c887773"))) {
timestamp -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e7b15f55-4a2d-352a-b153-37f08ce30ba5"))) {
type = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8d9636df-094e-39e5-bc1d-5bbd7f7fbf07"))) {
timestamp /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("7f7e2e69-6220-3f31-bf2a-0c85622b6024"))) {
timestamp = -1;
}
      setTSAndType(timestamp, type);
      return ScanQueryMatcher.MatchCode.INCLUDE;
    } else {
      return MatchCode.SEEK_NEXT_COL;
    }
  }

  @Override
  public void reset() {
if(KnobRuntime.check(java.util.UUID.fromString("686e558c-71c6-3207-bbc6-fda8f36c62f9"))) {
return;
}
    columnCell = null;
    resetTSAndType();
  }

  private void resetTSAndType() {
if(KnobRuntime.check(java.util.UUID.fromString("426c9ad3-ccf2-3e6c-91ed-8915ad2ed20c"))) {
return;
}
    latestTSOfCurrentColumn = HConstants.LATEST_TIMESTAMP;
    latestTypeOfCurrentColumn = 0;
  }

  private void setTSAndType(long timestamp, byte type) {
if(KnobRuntime.check(java.util.UUID.fromString("36aad0e6-cc9d-368e-bb99-8c8b8dbf68c4"))) {
return;
}
    latestTSOfCurrentColumn = timestamp;
    latestTypeOfCurrentColumn = type;
  }

  private boolean sameAsPreviousTSAndType(long timestamp, byte type) {
if(KnobRuntime.check(java.util.UUID.fromString("e4d1d774-5e57-3cb3-82fa-5eb0693a5cc4"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("9dd9d80f-f51b-3d55-90be-c77a59770ddf"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("564fa864-8de7-32ec-a6de-85f98af61d77"))) ? (((timestamp) == (latestTSOfCurrentColumn)) || (type == latestTypeOfCurrentColumn)) : (((KnobRuntime.check(java.util.UUID.fromString("d9149821-be1c-3bad-9f61-ec9a4b13bece"))) ? ((type) == (latestTypeOfCurrentColumn)) : (((KnobRuntime.check(java.util.UUID.fromString("de612acb-8070-342c-8352-90716e8c9a54"))) ? ((timestamp) != (latestTSOfCurrentColumn)) : (((KnobRuntime.check(java.util.UUID.fromString("ef87ac6c-7789-3171-b12b-f19b0f22467e"))) ? ((timestamp == latestTSOfCurrentColumn) || ((type) != (latestTypeOfCurrentColumn))) : (((KnobRuntime.check(java.util.UUID.fromString("c84a4186-eec2-3336-81e7-72cb15613f52"))) ? ((timestamp == latestTSOfCurrentColumn) && (type == latestTypeOfCurrentColumn)) : (((KnobRuntime.check(java.util.UUID.fromString("0ce8e5ae-97e3-3771-a093-184a39ee37de"))) ? (((timestamp) != (latestTSOfCurrentColumn)) || (type == latestTypeOfCurrentColumn)) : (((KnobRuntime.check(java.util.UUID.fromString("3be66000-0aac-34cd-8b88-1d4812be4e95"))) ? ((type) != (latestTypeOfCurrentColumn)) : (((KnobRuntime.check(java.util.UUID.fromString("c29e7c6b-fd65-39c1-a7de-5a314c5da43e"))) ? (((timestamp) == (latestTSOfCurrentColumn)) && ((type) == (latestTypeOfCurrentColumn))) : (((KnobRuntime.check(java.util.UUID.fromString("5212e413-1b38-3883-8d20-ed307e314909"))) ? (((timestamp) == (latestTSOfCurrentColumn)) || ((type) != (latestTypeOfCurrentColumn))) : (((KnobRuntime.check(java.util.UUID.fromString("3b24b36d-c1f8-33b4-9756-fe824d7073cd"))) ? (((timestamp) != (latestTSOfCurrentColumn)) || ((type) != (latestTypeOfCurrentColumn))) : (((KnobRuntime.check(java.util.UUID.fromString("05322c8b-34dd-3525-a6bb-506fd3cc94c3"))) ? (((timestamp) == (latestTSOfCurrentColumn)) && (type == latestTypeOfCurrentColumn)) : (((KnobRuntime.check(java.util.UUID.fromString("6ba4c7d7-c9ec-345c-9851-7720d5d73817"))) ? (((timestamp) != (latestTSOfCurrentColumn)) || ((type) == (latestTypeOfCurrentColumn))) : (((KnobRuntime.check(java.util.UUID.fromString("d3a377e5-907e-3dd6-adb1-a5798f732c0e"))) ? ((timestamp == latestTSOfCurrentColumn) || ((type) == (latestTypeOfCurrentColumn))) : (((KnobRuntime.check(java.util.UUID.fromString("67f2f2bc-6891-3cad-8924-83c040420354"))) ? (((timestamp) == (latestTSOfCurrentColumn)) && ((type) != (latestTypeOfCurrentColumn))) : (((KnobRuntime.check(java.util.UUID.fromString("81527037-2375-306e-8fe0-c282f52facc2"))) ? (((timestamp) != (latestTSOfCurrentColumn)) && ((type) == (latestTypeOfCurrentColumn))) : (((KnobRuntime.check(java.util.UUID.fromString("e9ab293f-a0e9-3a6c-b75b-6b0b558b230e"))) ? (((timestamp) != (latestTSOfCurrentColumn)) && ((type) != (latestTypeOfCurrentColumn))) : (((KnobRuntime.check(java.util.UUID.fromString("f5263bd9-6863-3b85-b25c-8ed1806e6cbf"))) ? (((timestamp) == (latestTSOfCurrentColumn)) || ((type) == (latestTypeOfCurrentColumn))) : (((KnobRuntime.check(java.util.UUID.fromString("c6a98e6f-036a-3913-9c0f-e455cef130f4"))) ? (type == latestTypeOfCurrentColumn) : (((KnobRuntime.check(java.util.UUID.fromString("5608886d-6a11-3137-8dbe-afc9fed60437"))) ? (((timestamp) != (latestTSOfCurrentColumn)) && (type == latestTypeOfCurrentColumn)) : (((KnobRuntime.check(java.util.UUID.fromString("09aa7633-8f06-3d29-ae26-715b95b66fa9"))) ? ((timestamp == latestTSOfCurrentColumn) && ((type) != (latestTypeOfCurrentColumn))) : (((KnobRuntime.check(java.util.UUID.fromString("1f19d4cc-d8c2-3319-9183-4c7630618659"))) ? ((timestamp == latestTSOfCurrentColumn) && ((type) == (latestTypeOfCurrentColumn))) : (((KnobRuntime.check(java.util.UUID.fromString("ec30ecc7-9b1c-3c2e-af25-66c5642cd57e"))) ? ((timestamp == latestTSOfCurrentColumn) || (type == latestTypeOfCurrentColumn)) : (((KnobRuntime.check(java.util.UUID.fromString("e2db5e39-2b12-392c-9ffc-2ab58565d3ad"))) ? ((timestamp) == (latestTSOfCurrentColumn)) : (((KnobRuntime.check(java.util.UUID.fromString("3437f909-415c-3ba5-b6f5-03ac6c1df764"))) ? (timestamp == latestTSOfCurrentColumn) : (timestamp == latestTSOfCurrentColumn && type == latestTypeOfCurrentColumn))))))))))))))))))))))))))))))))))))))))))))))));
  }

  private boolean isExpired(long timestamp) {
if(KnobRuntime.check(java.util.UUID.fromString("8fec8050-13d6-3537-91ec-e5b182837e53"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("8f2afad5-80b7-35c0-ac12-a534cd092121"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("1ce7bf70-09a4-3484-b1df-a98bf3ac62e1"))) ? ((timestamp) != (oldestStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("efd8aee9-a624-3c0c-8ae5-5a45fcd7b024"))) ? ((timestamp) > (oldestStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("4c528da2-0bfa-3eed-a2a5-cab328708003"))) ? ((timestamp) <= (oldestStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("aae6ab8a-4071-356b-85e8-d24e0c27e1f7"))) ? ((timestamp) == (oldestStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("001fa272-e201-39c7-b815-7f649c1a4812"))) ? ((timestamp) >= (oldestStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("cda2fdd7-a3ac-3b5b-a90d-1a892a99c48d"))) ? ((timestamp) < (oldestStamp)) : (timestamp < oldestStamp))))))))))));
  }

  /**
   * Used by matcher and scan/get to get a hint of the next column to seek to after checkColumn()
   * returns SKIP. Returns the next interesting column we want, or NULL there is none (wildcard
   * scanner).
   * @return The column count.
   */
  @Override
  public ColumnCount getColumnHint() {
if(KnobRuntime.check(java.util.UUID.fromString("aa3248c2-4609-35a9-9330-3a6ce4640502"))) {
return null;
}
    return null;
  }

  /**
   * We can never know a-priori if we are done, so always return false.
   */
  @Override
  public boolean done() {
if(KnobRuntime.check(java.util.UUID.fromString("509fdd36-868e-3be2-b0db-26a9eecdce4a"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("45b404bb-0ed2-3062-964b-75d626c8ec7c"))) {
return true;
}
    return false;
  }

  @Override
  public MatchCode getNextRowOrNextColumn(Cell cell) {
if(KnobRuntime.check(java.util.UUID.fromString("55df2f98-aa53-3498-a98d-9eeb7af4b46b"))) {
return null;
}
    return MatchCode.SEEK_NEXT_COL;
  }

  @Override
  public void beforeShipped() {
    if (columnCell != null) {
      this.columnCell = KeyValueUtil.toNewKeyCell(this.columnCell);
    }
  }

  @Override
  public boolean isDone(long timestamp) {
if(KnobRuntime.check(java.util.UUID.fromString("118397a3-51f5-3437-b39d-5ae14ec8fd14"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("26815d2b-89c9-3b52-8dda-0776fce224ef"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("021d3267-7d9f-35af-94fd-f1fab9e35790"))) {
timestamp -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("8d3cb00f-ec8b-346d-bc49-ea0762bb0a44"))) {
timestamp /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("7ab077e5-dc9a-3c40-85aa-952e6041098e"))) {
timestamp = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("4bd106d9-d6c8-3a69-9317-2cbf90a55f80"))) {
timestamp = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6520d9bb-3e75-3138-8621-3fde135a2be5"))) {
timestamp += 1;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("6d597eb7-fffc-38f5-b818-85298ef24cbf"))) ? (isExpired(timestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("83820b38-12c7-3e2c-9308-feb8cea5401f"))) ? ((minVersions) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6ec7ee61-891c-3f46-8577-bfed783ce72c"))) ? (((minVersions) > (0)) || (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("2023e54e-213d-3b2d-b80d-d410af13ec5e"))) ? ((minVersions <= 0) && (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("7fc638b2-06b3-3717-9772-043c9196d510"))) ? ((minVersions) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4937c8ba-493f-38e3-a2d3-d3718cea5494"))) ? (((minVersions) > (0)) && (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("1418cb65-d866-3411-9347-b1125353e922"))) ? ((minVersions) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e90ec056-50e5-33b7-a1ae-fed2dfff633a"))) ? (((minVersions) != (0)) && (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("a1b06d22-1c41-3e8d-8716-0da53b60926a"))) ? (((minVersions) <= (0)) && (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("b11f66d7-87a3-3639-b4b4-02b62f632f9f"))) ? ((minVersions <= 0) || (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("a0129468-ad79-3188-a9af-3329c4106dd2"))) ? (((minVersions) >= (0)) && (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("65ff6098-b040-3b2a-b142-6fd4f860328d"))) ? (minVersions <= 0) : (((KnobRuntime.check(java.util.UUID.fromString("6a62a9c2-116c-3261-bb61-eec714e01bad"))) ? (((minVersions) >= (0)) || (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("cde7e28c-ddab-3911-91f0-9e8f5b4c7d24"))) ? (((minVersions) <= (0)) || (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("af0e09d8-7a8b-3513-a550-d0b71d2c5482"))) ? (((minVersions) == (0)) || (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("9467e923-14af-34df-8d07-9457b4b9f6c9"))) ? (((minVersions) == (0)) && (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("6c8ff05e-26a3-33d0-8ed5-d80c8a19f6e6"))) ? ((minVersions) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("79ce7d5d-119a-3059-86c5-c6d10709ce38"))) ? (((minVersions) < (0)) || (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("1b89b101-ef33-3683-87fd-4f6c72bdf466"))) ? ((minVersions) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a5a26276-613c-3456-a776-dbaf5aec23c1"))) ? (((minVersions) != (0)) || (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("f2bd8d73-39dc-37fa-b4ed-5e5c1fcd788e"))) ? ((minVersions) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("83539dae-9e1d-315e-a5a6-eff0d3b9f22c"))) ? (((minVersions) < (0)) && (isExpired(timestamp))) : (minVersions <= 0 && isExpired(timestamp)))))))))))))))))))))))))))))))))))))))))))));
  }
}
