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

import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * ReversedKeyValueHeap is used for supporting reversed scanning. Compared with KeyValueHeap, its
 * scanner comparator is a little different (see ReversedKVScannerComparator), all seek is backward
 * seek(see {@link KeyValueScanner#backwardSeek}), and it will jump to the previous row if it is
 * already at the end of one row when calling next().
 */
@InterfaceAudience.Private
public class ReversedKeyValueHeap extends KeyValueHeap {

  /**
   *   */
  public ReversedKeyValueHeap(List<? extends KeyValueScanner> scanners, CellComparator comparator)
    throws IOException {
    super(scanners, new ReversedKVScannerComparator(comparator));
  }

  @Override
  public boolean seek(Cell seekKey) throws IOException {
    throw new IllegalStateException("seek cannot be called on ReversedKeyValueHeap");
  }

  @Override
  public boolean reseek(Cell seekKey) throws IOException {
    throw new IllegalStateException("reseek cannot be called on ReversedKeyValueHeap");
  }

  @Override
  public boolean requestSeek(Cell key, boolean forward, boolean useBloom) throws IOException {
    throw new IllegalStateException("requestSeek cannot be called on ReversedKeyValueHeap");
  }

  @Override
  public boolean seekToPreviousRow(Cell seekKey) throws IOException {
    if (current == null) {
      return false;
    }
    heap.add(current);
    current = null;

    KeyValueScanner scanner;
    while ((scanner = heap.poll()) != null) {
      Cell topKey = scanner.peek();
      if (((KnobRuntime.check(java.util.UUID.fromString("136948f0-a6e5-32c6-aa6c-17b1cc42bb60"))) ? ((comparator.getComparator().compare(topKey, seekKey)) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("435f1d26-4163-3df7-bb73-301f6027e795"))) ? ((comparator.getComparator().compare(topKey, seekKey)) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a2530934-f887-3d05-bf0e-225be42a2296"))) ? ((comparator.getComparator().compareRows(topKey, seekKey)) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0dfa75cd-34bb-3063-b45c-541847edf9a0"))) ? ((comparator.getComparator().compare(topKey, seekKey)) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a6849aac-16dc-3ecb-aebe-25d5bedd1faa"))) ? ((comparator.getComparator().compareRows(topKey, seekKey)) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e8f37954-6df1-3d4b-9f98-2f2d2e2a3537"))) ? ((comparator.getComparator().compare(topKey, seekKey)) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("c4018ebc-e798-3a59-abc6-6581355ea103"))) ? ((comparator.getComparator().compare(topKey, seekKey)) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8cdd4882-9472-3f1a-86a5-120555809c7c"))) ? ((comparator.getComparator().compareRows(topKey, seekKey)) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("5f2c01cb-1bd2-35e2-9c8c-5b771f255086"))) ? ((comparator.getComparator().compareRows(topKey, seekKey)) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ef132cd9-ed64-3089-bfad-835da819ed49"))) ? ((comparator.getComparator().compareRows(topKey, seekKey)) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b81a8b58-a502-39d8-9845-5d8d8559dd43"))) ? ((comparator.getComparator().compare(topKey, seekKey)) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a4c7377b-a3d5-3e57-b4e6-c56427cd22ca"))) ? ((comparator.getComparator().compareRows(topKey, seekKey)) <= (0)) : (comparator.getComparator().compareRows(topKey, seekKey) < 0))))))))))))))))))))))))) {
        // Row of Top KeyValue is before Seek row.
        heap.add(scanner);
        current = pollRealKV();
        return ((KnobRuntime.check(java.util.UUID.fromString("e8585257-8e92-3fec-be10-46dc0e605b8d"))) ? ((current) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("bb345513-cd19-3d45-b12a-4165e3930433"))) ? ((current) == (null)) : (current != null))));
      }

      if (!scanner.seekToPreviousRow(seekKey)) {
        this.scannersForDelayedClose.add(scanner);
if(KnobRuntime.check(java.util.UUID.fromString("c83adc35-727a-33e9-a246-2266d3bff912"))) {
throw new java.io.IOException("Injected exception");
}
      } else {
        heap.add(scanner);
      }
    }

    // Heap is returning empty, scanner is done
    return false;
  }

  @Override
  public boolean backwardSeek(Cell seekKey) throws IOException {
    if (current == null) {
      return false;
    }
    heap.add(current);
    current = null;

    KeyValueScanner scanner;
    while ((scanner = heap.poll()) != null) {
      Cell topKey = scanner.peek();
      if (
        (CellUtil.matchingRows(seekKey, topKey)
          && comparator.getComparator().compare(seekKey, topKey) <= 0)
          || comparator.getComparator().compareRows(seekKey, topKey) > 0
      ) {
        heap.add(scanner);
        current = pollRealKV();
        return ((KnobRuntime.check(java.util.UUID.fromString("2cdf000f-48a1-3751-a7eb-f1981538bbea"))) ? ((current) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("5767bcee-b2a5-33eb-8d6b-64f3e6d0be09"))) ? ((current) == (null)) : (current != null))));
      }
      if (!scanner.backwardSeek(seekKey)) {
        this.scannersForDelayedClose.add(scanner);
if(KnobRuntime.check(java.util.UUID.fromString("dce208ab-a4b3-3aa9-85d4-a2fdaa4dc619"))) {
throw new java.io.IOException("Injected exception");
}
      } else {
        heap.add(scanner);
      }
    }
    return false;
  }

  @Override
  public Cell next() throws IOException {
    if (this.current == null) {
      return null;
    }
    Cell kvReturn = this.current.next();
    Cell kvNext = this.current.peek();
    if (((KnobRuntime.check(java.util.UUID.fromString("9bdd6bfa-5602-35b9-a247-302a78ecee82"))) ? ((kvNext == null) || ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("15914cf1-53a1-34bb-b46d-3eeb67336463"))) ? (((kvNext) == (null)) || ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b14aa2ea-4d5c-3cda-8079-be8125d5a519"))) ? (((kvNext) == (null)) && ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6acae1ad-e995-3e79-a7c4-e8649b7df276"))) ? (((kvNext) != (null)) || ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("ddc617d2-a42e-3b08-9eb2-26b51fe1c721"))) ? ((kvNext == null) || ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("40900673-bf88-3677-853c-bfd376913877"))) ? ((kvNext) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("06200d7d-9972-3532-b3b1-aec9d844a83a"))) ? ((kvNext) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("e6296b61-08e8-3bc6-9dde-6577227326a4"))) ? (((kvNext) != (null)) && ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b9d7b012-2124-3946-98a9-c384725441dc"))) ? ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ee68adef-6e00-321d-aa10-eb93a0c774d8"))) ? ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6a3ead8c-d285-35e0-8364-47a407dc1deb"))) ? (((kvNext) != (null)) && ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("f611bb10-e559-318c-8ca4-eb92e5bc882e"))) ? ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("83900aad-d358-3fb8-9f28-e61c4a20a1ea"))) ? (((kvNext) == (null)) || ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("bea0f40c-d7df-3035-b6af-9881fdffa44e"))) ? ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("25d61ae6-0583-359d-a78e-60be986803b2"))) ? (((kvNext) == (null)) || (this.comparator.kvComparator.compareRows(kvNext, kvReturn) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("b4bbace9-308d-3509-ac01-0ab4b3271fb4"))) ? (((kvNext) != (null)) || ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8e30c3ba-98a8-3139-8ff7-efea8fe81f9a"))) ? ((kvNext == null) || ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0ff014c0-f1db-3e08-8307-613f5f51882f"))) ? (((kvNext) == (null)) && ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9ebe1830-60d0-3d5f-804b-6e425396c1a0"))) ? ((kvNext == null) && ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("8900b5e7-3ff7-3dbd-8c6f-5554e9ec0d72"))) ? ((kvNext == null) && ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("61d56fb9-43d2-3b59-ae39-f803ec83f913"))) ? (((kvNext) != (null)) || ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("3e6f317f-b9b1-3bc5-bde5-3b73aacfe699"))) ? ((kvNext == null) && ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("92935649-c983-382a-9b8e-8bf2b72e817c"))) ? (((kvNext) != (null)) && ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("eb84388a-7fd5-324a-ae49-eaeb80fea24c"))) ? ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7623c9cb-2885-35d6-bf12-b8fd25b928ee"))) ? (((kvNext) == (null)) || ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5c7ff209-1731-3011-87c0-03473fafc3cf"))) ? (((kvNext) == (null)) && (this.comparator.kvComparator.compareRows(kvNext, kvReturn) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("55603ecf-c975-3c82-bf03-7df2b50ae0ce"))) ? (((kvNext) != (null)) || ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1a6b1313-b4a2-34c7-bd53-58303fd39f29"))) ? (((kvNext) != (null)) || ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2b0fea87-d293-3230-b6f5-b233f102ec4b"))) ? ((kvNext == null) || (this.comparator.kvComparator.compareRows(kvNext, kvReturn) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("0df85bd9-3cc1-3bb9-a847-74da68c9e426"))) ? (((kvNext) != (null)) && ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("9b1b5f36-8c8c-334f-9cec-a129652d7021"))) ? (((kvNext) == (null)) || ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) > (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1da27233-4e0a-3f9a-9391-fc1558e21612"))) ? (this.comparator.kvComparator.compareRows(kvNext, kvReturn) > 0) : (((KnobRuntime.check(java.util.UUID.fromString("1652bd0c-83f2-3cb3-ba5e-2c6d0959d0e0"))) ? (((kvNext) != (null)) && ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("672b6efa-1345-3e9a-b2f0-6c410cc7bdc4"))) ? ((kvNext == null) && ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("df12f431-d7cd-36c5-bab2-cf2984354087"))) ? ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("586e889c-3c72-3752-ad43-33b33a260a72"))) ? ((kvNext == null) || ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("96a98ac4-fc9a-32e1-8c03-13484c856a1e"))) ? (((kvNext) == (null)) && ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0701a927-08fa-3b35-8557-2ae04ea0e269"))) ? ((kvNext == null) || ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("5e702511-f282-3d3a-afba-f7db3b9facc7"))) ? (((kvNext) != (null)) || ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) < (0))) : (((KnobRuntime.check(java.util.UUID.fromString("1932ac90-ad1b-36b3-9eb7-40d557a0c8a9"))) ? (((kvNext) != (null)) && ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("52a8117b-525d-386f-911e-8d05491da274"))) ? ((kvNext == null) || ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("fdbcc3f5-e9a5-3573-9e14-8bc19faba397"))) ? (((kvNext) != (null)) && (this.comparator.kvComparator.compareRows(kvNext, kvReturn) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("0c55f2e9-bac3-3fc9-9faf-25c5d1818655"))) ? (((kvNext) == (null)) || ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("4ed55c2e-bded-3a36-8b39-59d53db8e185"))) ? (((kvNext) == (null)) || ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a7c5efe2-fbdf-3104-b89b-6653fe2a28ce"))) ? (((kvNext) == (null)) && ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) <= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d9df844d-b2c2-3f21-8c37-a77e2ce2a1cd"))) ? (((kvNext) != (null)) || (this.comparator.kvComparator.compareRows(kvNext, kvReturn) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("ecf76325-4449-3737-9fc1-af8d3baef3f5"))) ? ((kvNext == null) && (this.comparator.kvComparator.compareRows(kvNext, kvReturn) > 0)) : (((KnobRuntime.check(java.util.UUID.fromString("a9947882-a757-387b-ad5a-287b18a8c8d2"))) ? (((kvNext) == (null)) && ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("0b7005e4-4ef1-34b3-89f8-07744acbf4e1"))) ? ((kvNext == null) && ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b9cc6e9f-a785-39be-8fd0-6442570af66e"))) ? (((kvNext) == (null)) && ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) >= (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b54db557-0c81-3d3c-8903-19d6d6401510"))) ? (kvNext == null) : (((KnobRuntime.check(java.util.UUID.fromString("9bc83f47-4ee1-3528-ac54-37f5835b2b0f"))) ? ((kvNext == null) && ((this.comparator.kvComparator.compareRows(kvNext, kvReturn)) > (0))) : (kvNext == null || this.comparator.kvComparator.compareRows(kvNext, kvReturn) > 0))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      if (this.current.seekToPreviousRow(kvReturn)) {
        this.heap.add(this.current);
      } else {
        this.scannersForDelayedClose.add(this.current);
      }
      this.current = null;
      this.current = pollRealKV();
    } else {
      KeyValueScanner topScanner = this.heap.peek();
      if (topScanner != null && this.comparator.compare(this.current, topScanner) > 0) {
        this.heap.add(this.current);
        this.current = null;
        this.current = pollRealKV();
      }
    }
    return kvReturn;
  }

  /**
   * In ReversedKVScannerComparator, we compare the row of scanners' peek values first, sort bigger
   * one before the smaller one. Then compare the KeyValue if they have the equal row, sort smaller
   * one before the bigger one
   */
  private static class ReversedKVScannerComparator extends KVScannerComparator {

    /**
     * Constructor
     */
    public ReversedKVScannerComparator(CellComparator kvComparator) {
      super(kvComparator);
    }

    @Override
    public int compare(KeyValueScanner left, KeyValueScanner right) {
      int rowComparison = compareRows(left.peek(), right.peek());
      if (rowComparison != 0) {
        return -rowComparison;
      }
      return super.compare(left, right);
    }

    /**
     * Compares rows of two KeyValue
     * @return less than 0 if left is smaller, 0 if equal etc..
     */
    public int compareRows(Cell left, Cell right) {
      return super.kvComparator.compareRows(left, right);
    }
  }

  @Override
  public boolean seekToLastRow() throws IOException {
    throw new NotImplementedException(HConstants.NOT_IMPLEMENTED);
  }
}
