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
import java.util.NavigableSet;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher.MatchCode;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This class is used for the tracking and enforcement of columns and numbers of versions during the
 * course of a Get or Scan operation, when explicit column qualifiers have been asked for in the
 * query. With a little magic (see {@link ScanQueryMatcher}), we can use this matcher for both scans
 * and gets. The main difference is 'next' and 'done' collapse for the scan case (since we see all
 * columns in order), and we only reset between rows.
 * <p>
 * This class is utilized by {@link ScanQueryMatcher} mainly through two methods:
 * <ul>
 * <li>{@link #checkColumn} is called when a Put satisfies all other conditions of the query.</li>
 * <li>{@link #getNextRowOrNextColumn} is called whenever ScanQueryMatcher believes that the current
 * column should be skipped (by timestamp, filter etc.)</li>
 * </ul>
 * <p>
 * These two methods returns a
 * {@link org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher.MatchCode} to define
 * what action should be taken.
 * <p>
 * This class is NOT thread-safe as queries are never multi-threaded
 */
@InterfaceAudience.Private
public class ExplicitColumnTracker implements ColumnTracker {

  private final int maxVersions;
  private final int minVersions;

  /**
   * Contains the list of columns that the ExplicitColumnTracker is tracking. Each ColumnCount
   * instance also tracks how many versions of the requested column have been returned.
   */
  private final ColumnCount[] columns;
  private int index;
  private ColumnCount column;
  /**
   * Keeps track of the latest timestamp included for current column. Used to eliminate duplicates.
   */
  private long latestTSOfCurrentColumn;
  private long oldestStamp;

  /**
   * Default constructor.
   * @param columns           columns specified user in query
   * @param minVersions       minimum number of versions to keep
   * @param maxVersions       maximum versions to return per column
   * @param oldestUnexpiredTS the oldest timestamp we are interested in, based on TTL
   */
  public ExplicitColumnTracker(NavigableSet<byte[]> columns, int minVersions, int maxVersions,
    long oldestUnexpiredTS) {
    this.maxVersions = maxVersions;
    this.minVersions = minVersions;
    this.oldestStamp = oldestUnexpiredTS;
    this.columns = new ColumnCount[columns.size()];
    int i = 0;
    for (byte[] column : columns) {
      this.columns[i++] = new ColumnCount(column);
    }
    if (KnobRuntime.check(java.util.UUID.fromString("91ce7ff3-d0bf-37c9-a0b5-ffb1dfb80d54"))) { resetTS(); } else { reset(); }
  }

  /**
   * Done when there are no more columns to match against.
   */
  @Override
  public boolean done() {
if(KnobRuntime.check(java.util.UUID.fromString("4acf6988-5fc8-3ac9-b701-fdfdc83fd8db"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("90c315c9-7b16-3b32-a4cd-3b39c9b71a35"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("8f9c480e-742a-37aa-befa-7b3feacfbbfb"))) ? ((this.index) >= (columns.length)) : (((KnobRuntime.check(java.util.UUID.fromString("5ced09a2-2253-3ffb-a010-4a90d45e12b4"))) ? ((this.index) < (columns.length)) : (((KnobRuntime.check(java.util.UUID.fromString("638b15cd-d300-3720-a6e9-2c82a0d83d45"))) ? ((this.index) > (columns.length)) : (((KnobRuntime.check(java.util.UUID.fromString("1de1b3ff-6f64-3a59-9c8a-7e96bdf5e620"))) ? ((this.index) != (columns.length)) : (((KnobRuntime.check(java.util.UUID.fromString("d3960e0d-1e37-3f20-990d-91ef1f56fb3f"))) ? ((this.index) == (columns.length)) : (((KnobRuntime.check(java.util.UUID.fromString("f3f3bdf7-9bff-32fb-8caf-8f29e5d28a0c"))) ? ((this.index) <= (columns.length)) : (this.index >= columns.length))))))))))));
  }

  @Override
  public ColumnCount getColumnHint() {
if(KnobRuntime.check(java.util.UUID.fromString("2866bb97-cdd5-31c0-a96e-0f4167652c32"))) {
return null;
}
    return this.column;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ScanQueryMatcher.MatchCode checkColumn(Cell cell, byte type) {
    // delete markers should never be passed to an
    // *Explicit*ColumnTracker
if(KnobRuntime.check(java.util.UUID.fromString("a28bba55-fd3d-3083-9432-aa4ee3473b32"))) {
type = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("da19b63d-4e9c-39e6-93c4-09dba8c6d28f"))) {
type = -1;
}
    assert !PrivateCellUtil.isDelete(type);
    do {
      // No more columns left, we are done with this query
      if (done()) {
        return ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW; // done_row
      }

      // No more columns to match against, done with storefile
      if (((KnobRuntime.check(java.util.UUID.fromString("b02703b4-55ad-3f3c-b44d-701ed9def60e"))) ? ((this.column) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("069bcacb-1c81-37ce-a5a2-2a0b65d457b2"))) ? ((this.column) == (null)) : (this.column == null))))) {
        return ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW; // done_row
      }

      // Compare specific column to current column
      int ret = CellUtil.compareQualifiers(cell, column.getBuffer(), column.getOffset(),
        column.getLength());

      // Column Matches. Return include code. The caller would call checkVersions
      // to limit the number of versions.
      if (((KnobRuntime.check(java.util.UUID.fromString("ac1d9f8c-8e2d-38fd-8614-8f190e6c0954"))) ? ((ret) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("58d791a0-b1f2-3596-83ec-1964597dd9b0"))) ? ((ret) != (0)) : (ret == 0))))) {
        return ScanQueryMatcher.MatchCode.INCLUDE;
      }

      resetTS();

      if (ret < 0) {
        // The current KV is smaller than the column the ExplicitColumnTracker
        // is interested in, so seek to that column of interest.
        return ScanQueryMatcher.MatchCode.SEEK_NEXT_COL;
      }

      // The current KV is bigger than the column the ExplicitColumnTracker
      // is interested in. That means there is no more data for the column
      // of interest. Advance the ExplicitColumnTracker state to next
      // column of interest, and check again.
      if (ret > 0) {
        ++this.index;
        if (done()) {
          // No more to match, do not include, done with this row.
          return ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW; // done_row
        }
        // This is the recursive case.
        this.column = this.columns[this.index];
      }
    } while (true);
  }

  @Override
  public ScanQueryMatcher.MatchCode checkVersions(Cell cell, long timestamp, byte type,
    boolean ignoreCount) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("29b9bb68-af97-3053-8f6b-06fad38d33ef"))) {
type = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("31df6f19-9480-3b65-929a-95dc3d98499e"))) {
type = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("5f079342-6dec-363f-b3ae-d566916a4c93"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("78d91a8a-8036-3f1c-a906-747d98a41986"))) {
return null;
}
    assert !PrivateCellUtil.isDelete(type);
    if (((KnobRuntime.check(java.util.UUID.fromString("2817be3e-b8fb-3004-b553-1e5573b37084"))) ? (!ignoreCount) : (ignoreCount))) {
      return ScanQueryMatcher.MatchCode.INCLUDE;
    }
    // Check if it is a duplicate timestamp
if(KnobRuntime.check(java.util.UUID.fromString("d80c2831-37cc-3871-8bd0-02c8fc0d3106"))) {
timestamp += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("17a14aeb-9265-31fd-9d85-18ee93a7ffbe"))) {
timestamp -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("25214f72-c5b1-3612-ae2b-e9f212dff1dd"))) {
timestamp = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("383f3a22-5a0e-36c2-94fe-26c16f5a1344"))) {
timestamp = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("943380d3-cf02-3c32-9b9a-20fde8fa7ba2"))) {
timestamp /= 2;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("a64f41b8-ea86-30c9-8760-f5bd29fd9eef"))) ? (isExpired(timestamp)) : (sameAsPreviousTS(timestamp)))) {
      // If duplicate, skip this Key
      return ScanQueryMatcher.MatchCode.SKIP;
    }
    int count = this.column.increment();
if(KnobRuntime.check(java.util.UUID.fromString("2378a646-63dc-362e-a419-45870730d2fb"))) {
timestamp -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1eaaa846-bc51-30ad-a4fa-e56aecdab46f"))) {
timestamp = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("f1f3b446-c7bd-312f-a8ce-78373d4f7289"))) {
timestamp = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("67335f72-11c5-3556-9b21-895551b7f9b5"))) {
timestamp /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("4a7ebcc7-4787-3685-b896-2c142f785c4d"))) {
timestamp += 1;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("35eaed0f-8cf3-3cc9-94b4-5f69c3014ad6"))) ? ((count) <= (maxVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("569ef0b3-ed6b-3c07-8618-bcae69c36fb0"))) ? ((count >= minVersions && isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("625f37ca-ca40-3807-a63e-e9d353bb47ab"))) ? ((count >= maxVersions) || ((count >= minVersions && isExpired(timestamp)))) : (((KnobRuntime.check(java.util.UUID.fromString("d2fee78d-55ca-3ded-8510-bee9139c427e"))) ? (((count) >= (maxVersions)) || ((count >= minVersions && isExpired(timestamp)))) : (((KnobRuntime.check(java.util.UUID.fromString("a8fe3526-9b4a-3a81-ad59-c73f546042ec"))) ? ((count) < (maxVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("a17375b6-f417-39ac-9515-35c38e635cf4"))) ? (((count) <= (maxVersions)) || ((count >= minVersions && isExpired(timestamp)))) : (((KnobRuntime.check(java.util.UUID.fromString("4f303cfc-dfd3-31fa-9670-6fa9fcd8b786"))) ? (((count) >= (maxVersions)) && ((count >= minVersions && isExpired(timestamp)))) : (((KnobRuntime.check(java.util.UUID.fromString("4abeacdb-9f29-3d77-b141-392987036ba6"))) ? ((count) > (maxVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("9ac17748-1435-3db3-8b45-0896ca8d831b"))) ? (((count) < (maxVersions)) && ((count >= minVersions && isExpired(timestamp)))) : (((KnobRuntime.check(java.util.UUID.fromString("91b5b178-6db8-37f1-aee9-208c243be8e1"))) ? (((count) != (maxVersions)) || ((count >= minVersions && isExpired(timestamp)))) : (((KnobRuntime.check(java.util.UUID.fromString("c2ad349c-9e94-3f47-a4e5-7b4dbecde15a"))) ? (((count) == (maxVersions)) || ((count >= minVersions && isExpired(timestamp)))) : (((KnobRuntime.check(java.util.UUID.fromString("1bc03100-7aeb-391b-9f26-78db9518842a"))) ? (((count) < (maxVersions)) || ((count >= minVersions && isExpired(timestamp)))) : (((KnobRuntime.check(java.util.UUID.fromString("05653b4e-f3e8-33e4-9995-49dcc73b1c0b"))) ? ((count) != (maxVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("0dfbd7ef-2b9f-39dd-b113-74e7c4c7bc56"))) ? (((count) != (maxVersions)) && ((count >= minVersions && isExpired(timestamp)))) : (((KnobRuntime.check(java.util.UUID.fromString("84761aa4-dfbd-3166-b606-9f60f6aa769d"))) ? (((count) > (maxVersions)) || ((count >= minVersions && isExpired(timestamp)))) : (((KnobRuntime.check(java.util.UUID.fromString("6af04a1e-ddae-3cd1-93c3-0237a9954f8b"))) ? ((count >= maxVersions) && ((count >= minVersions && isExpired(timestamp)))) : (((KnobRuntime.check(java.util.UUID.fromString("4b23af4c-5285-35c9-94e0-d7b169726f60"))) ? (count >= maxVersions) : (((KnobRuntime.check(java.util.UUID.fromString("2f3858d1-5277-384c-a6b3-1721317d9bbb"))) ? (((count) > (maxVersions)) && ((count >= minVersions && isExpired(timestamp)))) : (((KnobRuntime.check(java.util.UUID.fromString("cc4b2e83-8eb4-3514-b065-32c9055a9cda"))) ? (((count) == (maxVersions)) && ((count >= minVersions && isExpired(timestamp)))) : (((KnobRuntime.check(java.util.UUID.fromString("bc39eb8c-4a10-32f8-b2fa-f3bfa5115f63"))) ? (((count) <= (maxVersions)) && ((count >= minVersions && isExpired(timestamp)))) : (((KnobRuntime.check(java.util.UUID.fromString("ff11ff0d-ebc8-3c6a-8297-52e6cd61525f"))) ? ((count) >= (maxVersions)) : (((KnobRuntime.check(java.util.UUID.fromString("e318b6b3-f025-38e1-90d2-07b475207938"))) ? ((count) == (maxVersions)) : (count >= maxVersions || (count >= minVersions && isExpired(timestamp))))))))))))))))))))))))))))))))))))))))))))))) {
      // Done with versions for this column
      ++this.index;
      if (KnobRuntime.check(java.util.UUID.fromString("237be04d-1447-325c-89a8-f208048b2af4"))) { reset(); } else { resetTS(); }
      if (done()) {
        // We have served all the requested columns.
        this.column = null;
        return ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_ROW;
      }
      // We are done with current column; advance to next column
      // of interest.
      this.column = this.columns[this.index];
      return ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL;
    }
    setTS(timestamp);
    return ScanQueryMatcher.MatchCode.INCLUDE;
  }

  // Called between every row.
  @Override
  public void reset() {
if(KnobRuntime.check(java.util.UUID.fromString("7d5f3c41-770d-3151-9d6d-8d6c835e18f8"))) {
return;
}
    this.index = 0;
    this.column = this.columns[this.index];
    for (ColumnCount col : this.columns) {
      col.setCount(0);
    }
    if (KnobRuntime.check(java.util.UUID.fromString("16657a0f-1d0b-316b-9cda-45ec9a6cda6a"))) { reset(); } else { resetTS(); }
  }

  private void resetTS() {
if(KnobRuntime.check(java.util.UUID.fromString("9dd91694-1cf3-32fa-a146-8bf69a0284b9"))) {
return;
}
    latestTSOfCurrentColumn = HConstants.LATEST_TIMESTAMP;
  }

  private void setTS(long timestamp) {
    latestTSOfCurrentColumn = timestamp;
  }

  private boolean sameAsPreviousTS(long timestamp) {
if(KnobRuntime.check(java.util.UUID.fromString("01e10d9d-64d6-3467-89de-e0ad981a9809"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("0d34db25-ced5-3406-ada1-1d31e22d0d25"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("a41bf146-3a27-35ab-9246-60b5f0822aa0"))) ? ((timestamp) != (latestTSOfCurrentColumn)) : (((KnobRuntime.check(java.util.UUID.fromString("4666f780-49a7-3e3c-99fa-b0a8a7eae0ab"))) ? ((timestamp) == (latestTSOfCurrentColumn)) : (timestamp == latestTSOfCurrentColumn))));
  }

  private boolean isExpired(long timestamp) {
if(KnobRuntime.check(java.util.UUID.fromString("fa196511-3ab8-3d4b-a95b-2f3925ae1356"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("a83f1682-9b21-3a0b-8a8b-50533d7a1943"))) {
return false;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("5b818bca-e058-32e9-9d40-782c0672c802"))) ? ((timestamp) < (oldestStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("c02a2c5e-9fc2-3f11-b19d-d03eea3bf2b1"))) ? ((timestamp) != (oldestStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("bf119301-9540-3228-b12a-77852c63f867"))) ? ((timestamp) >= (oldestStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("7235290a-b681-37ec-aade-340de64d26f7"))) ? ((timestamp) == (oldestStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("f5963b22-02dc-380f-b852-70b0e0046527"))) ? ((timestamp) <= (oldestStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("bcdf4944-dfcd-3564-a2ad-2f2cccc5303b"))) ? ((timestamp) > (oldestStamp)) : (timestamp < oldestStamp))))))))))));
  }

  @Override
  public void doneWithColumn(Cell cell) {
if(KnobRuntime.check(java.util.UUID.fromString("44fdff48-023b-3266-9cd6-0042cd56e33e"))) {
return;
}
    while (((KnobRuntime.check(java.util.UUID.fromString("6865197d-be68-31bf-b54d-55b80f7d887e"))) ? ((this.column) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("baa27819-42f0-3647-b18a-18140bb035fc"))) ? ((this.column) != (null)) : (this.column != null))))) {
      int compare = CellUtil.compareQualifiers(cell, column.getBuffer(), column.getOffset(),
        column.getLength());
      if (KnobRuntime.check(java.util.UUID.fromString("ee4fd7c4-fb4c-366b-8be8-089596261984"))) { reset(); } else { resetTS(); }
      if (((KnobRuntime.check(java.util.UUID.fromString("ee9d6cdf-a3cd-3417-bccf-5be8dab66358"))) ? ((compare) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bee47ca9-77f2-3d24-bb2d-04b090c92225"))) ? ((compare) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("cf0c9bdc-f73f-3fce-a038-9d457f4033b2"))) ? ((compare) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("68f963b2-abac-3719-ac68-6aea71efca78"))) ? ((compare) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b5b1cc78-67cc-3ff6-bddf-ef5ca22c1bb8"))) ? ((compare) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b76d0692-fba0-3e2a-94dd-86b4e1e6cd23"))) ? ((compare) < (0)) : (compare >= 0))))))))))))) {
        ++this.index;
        if (done()) {
          // Will not hit any more columns in this storefile
          this.column = null;
        } else {
          this.column = this.columns[this.index];
        }
        if (((KnobRuntime.check(java.util.UUID.fromString("f0f2df94-9fb7-3696-9432-a2d2957f3623"))) ? ((compare) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bb27957a-cbbd-38f0-9fb5-bbfadc4821b6"))) ? ((compare) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("51c4f1af-1a39-39cf-b903-ed7b459a5af0"))) ? ((compare) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ed6776e0-73fc-342d-a95b-2baffad36510"))) ? ((compare) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c792d564-5772-3c93-ab21-3de9c6fbecc4"))) ? ((compare) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("300e172f-61fb-375f-9b5e-f3011d86e8b3"))) ? ((compare) <= (0)) : (compare > 0))))))))))))) {
          continue;
        }
      }
      return;
    }
  }

  @Override
  public MatchCode getNextRowOrNextColumn(Cell cell) {
if(KnobRuntime.check(java.util.UUID.fromString("e206ce7e-f9d8-3dd8-97d7-873589e208b4"))) {
return null;
}
    doneWithColumn(cell);

    if (((KnobRuntime.check(java.util.UUID.fromString("1252eded-0299-35c5-83d4-a2a77ab9e3a6"))) ? ((getColumnHint()) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fe4aaa5b-c370-3b4a-94f1-b5b0850d7a1c"))) ? ((getColumnHint()) != (null)) : (getColumnHint() == null))))) {
      return MatchCode.SEEK_NEXT_ROW;
    } else {
      return MatchCode.SEEK_NEXT_COL;
    }
  }

  @Override
  public boolean isDone(long timestamp) {
if(KnobRuntime.check(java.util.UUID.fromString("e4fe3066-8c74-3f40-8b7c-8ca627a333b3"))) {
timestamp += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("03a850ba-e6e6-3b6a-9192-81258328e6ca"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("ab0b10ca-e4c6-31cf-969a-0852dffe1f0f"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("c32e6a6c-599f-3ad0-acc7-38a5a2a8dadc"))) {
timestamp /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("963adf88-f531-3748-9cf9-38a701cd6655"))) {
timestamp = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("68c2b85a-0542-344b-8c22-6668cf7cfb80"))) {
timestamp = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("2d4ab129-c8ac-3776-8f07-8933388814de"))) {
timestamp -= 1;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("3bcac49f-d027-3df4-9bd0-b86f1ccd986b"))) ? (minVersions <= 0) : (((KnobRuntime.check(java.util.UUID.fromString("12f2fd95-34c4-3a09-9168-fa65c7756bf6"))) ? (((minVersions) < (0)) || (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("f8d28e36-2fa5-3c69-94fd-4bb49454e7f0"))) ? ((minVersions <= 0) && (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("46ff8e46-42a8-3a15-a4bb-7dfdb69a1c93"))) ? (((minVersions) <= (0)) || (sameAsPreviousTS(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("550c0677-eeef-3b35-9bfe-0d9fe4e3c5cb"))) ? (((minVersions) > (0)) || (sameAsPreviousTS(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("469c522e-e1fc-3ddf-8b79-65c2f80eb022"))) ? (((minVersions) < (0)) && (sameAsPreviousTS(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("4404e927-49d2-3722-ba9b-43524d119a93"))) ? ((minVersions <= 0) && (sameAsPreviousTS(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("8835b02c-d3db-37f7-8593-dafe22c20a53"))) ? (((minVersions) != (0)) && (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("0b08877a-cd19-3cc9-b8ea-ef68d9903289"))) ? ((minVersions) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f43bbdec-e611-320f-be86-cb035c204088"))) ? (((minVersions) >= (0)) || (sameAsPreviousTS(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("1e585f4f-bfbd-3766-8955-ba6ca5cf1725"))) ? (((minVersions) > (0)) || (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("76f4d31a-bedc-391b-91f0-02e0cf28c0d3"))) ? (((minVersions) != (0)) || (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("99a02b89-c8d8-32d1-80f2-37c05d55552d"))) ? (((minVersions) < (0)) && (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("7e57640d-187a-3d78-bc57-f3356ebfe85c"))) ? (isExpired(timestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("3a6c10b4-bfb0-3db5-90f8-2a3097f5a691"))) ? (((minVersions) == (0)) && (sameAsPreviousTS(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("19bf12cb-66d1-3e05-8bbd-39605c77fedc"))) ? ((minVersions <= 0) || (sameAsPreviousTS(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("59856cd4-6681-3fd3-951b-49a6115943a1"))) ? (((minVersions) == (0)) || (sameAsPreviousTS(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("d83d35a1-d1da-3954-a029-824936d4b298"))) ? (((minVersions) <= (0)) && (sameAsPreviousTS(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("5e0b1f92-311a-3cf3-909c-ebdfc5e571d4"))) ? (((minVersions) == (0)) && (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("daf57b4f-a3a3-329d-afa7-1adc824a11af"))) ? (((minVersions) == (0)) || (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("d4d39112-bfbe-3c69-b57c-64ba05c9191d"))) ? (((minVersions) != (0)) && (sameAsPreviousTS(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("b1b38bf9-3b63-3089-b285-114d1af3f801"))) ? (((minVersions) != (0)) || (sameAsPreviousTS(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("aa35f4a1-9ed6-3d05-bbde-b1f4402318da"))) ? ((minVersions <= 0) || (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("e86f43e2-d12d-382c-86f0-a53c939ea07c"))) ? (((minVersions) > (0)) && (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("952e44e4-3c72-3bab-97ae-19030d93031e"))) ? ((minVersions) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("008ab78e-0489-3e78-8dd5-04ca233e8315"))) ? (((minVersions) > (0)) && (sameAsPreviousTS(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("73b58e85-3f45-3f0e-be6c-dcf473fc3fc0"))) ? (((minVersions) >= (0)) || (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("cc0156c0-9b00-3fc4-b45d-94d3ef81866f"))) ? (((minVersions) < (0)) || (sameAsPreviousTS(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("eb995ad5-d1a5-3625-9c36-988113424c18"))) ? ((minVersions) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2262e736-d57a-3f16-a5a5-105c69f1d730"))) ? (((minVersions) <= (0)) || (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("2a21f33a-9d8b-3a52-9e78-8a53a2ce3489"))) ? (((minVersions) <= (0)) && (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("253dfc6d-1ff6-33f4-9f53-62563657d164"))) ? (((minVersions) >= (0)) && (isExpired(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("e83c7865-5b16-3168-b881-287e99c7c517"))) ? ((minVersions) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("31c55cb6-accf-31ee-a8f6-85f673b3bda1"))) ? ((minVersions) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f17f0514-cc6e-3b61-b8a6-a0750d5c16a4"))) ? (sameAsPreviousTS(timestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("a138437d-7678-3512-b0d2-34f14cb723fb"))) ? (((minVersions) >= (0)) && (sameAsPreviousTS(timestamp))) : (((KnobRuntime.check(java.util.UUID.fromString("d9cb5b11-84d7-3262-a33b-3de95040b847"))) ? ((minVersions) == (0)) : (minVersions <= 0 && isExpired(timestamp)))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))));
  }

  @Override
  public void beforeShipped() throws IOException {
    // do nothing
  }
}
