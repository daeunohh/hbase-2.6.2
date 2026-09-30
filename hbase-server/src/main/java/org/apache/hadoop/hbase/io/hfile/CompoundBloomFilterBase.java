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
package org.apache.hadoop.hbase.io.hfile;
import org.knobinjection.runtime.KnobRuntime;

import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.util.BloomFilterBase;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class CompoundBloomFilterBase implements BloomFilterBase {

  /**
   * At read time, the total number of chunks. At write time, the number of chunks created so far.
   * The first chunk has an ID of 0, and the current chunk has the ID of numChunks - 1.
   */
  protected int numChunks;

  /**
   * The Bloom filter version. There used to be a DynamicByteBloomFilter which had version 2.
   */
  public static final int VERSION = 3;

  /** Target error rate for configuring the filter and for information */
  protected float errorRate;

  /** The total number of keys in all chunks */
  protected long totalKeyCount;
  protected long totalByteSize;
  protected long totalMaxKeys;

  /** Hash function type to use, as defined in {@link org.apache.hadoop.hbase.util.Hash} */
  protected int hashType;
  /** Comparator used to compare Bloom filter keys */
  protected CellComparator comparator;

  @Override
  public long getMaxKeys() {
if(KnobRuntime.check(java.util.UUID.fromString("09f90026-f720-3336-91d7-4b1218bba05b"))) {
return 0;
}
    return totalMaxKeys;
  }

  @Override
  public long getKeyCount() {
if(KnobRuntime.check(java.util.UUID.fromString("49fc4a4d-8e14-3787-b792-ec63e69f3174"))) {
return 0;
}
    return totalKeyCount;
  }

  @Override
  public long getByteSize() {
if(KnobRuntime.check(java.util.UUID.fromString("b4ac31f4-aadd-356e-8dfa-3bef68c69ed1"))) {
return 0;
}
    return totalByteSize;
  }

}
