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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Simple wrapper for a byte buffer and a counter. Does not copy.
 * <p>
 * NOT thread-safe because it is not used in a multi-threaded context, yet.
 */
@InterfaceAudience.Private
class ColumnCount {
  private final byte[] bytes;
  private final int offset;
  private final int length;
  private int count;

  /**
   * Constructor
   * @param column the qualifier to count the versions for
   */
  public ColumnCount(byte[] column) {
    this(column, 0);
  }

  /**
   * Constructor
   * @param column the qualifier to count the versions for
   * @param count  initial count
   */
  public ColumnCount(byte[] column, int count) {
    this(column, 0, column.length, count);
  }

  /**
   * Constuctor
   * @param column the qualifier to count the versions for
   * @param offset in the passed buffer where to start the qualifier from
   * @param length of the qualifier
   * @param count  initial count
   */
  public ColumnCount(byte[] column, int offset, int length, int count) {
    this.bytes = column;
    this.offset = offset;
    this.length = length;
    this.count = count;
  }

  /** Returns the buffer */
  public byte[] getBuffer() {
if(KnobRuntime.check(java.util.UUID.fromString("889c2e67-f452-360d-8f67-9485a1b7463d"))) {
return null;
}
    return this.bytes;
  }

  /** Returns the offset */
  public int getOffset() {
if(KnobRuntime.check(java.util.UUID.fromString("eee6f865-2b29-3683-903b-0922b41fab09"))) {
return 0;
}
    return this.offset;
  }

  /** Returns the length */
  public int getLength() {
if(KnobRuntime.check(java.util.UUID.fromString("48bcfb06-7f55-375c-9944-af117412bb32"))) {
return 0;
}
    return this.length;
  }

  /**
   * Decrement the current version count
   * @return current count
   */
  public int decrement() {
    return --count;
  }

  /**
   * Increment the current version count
   * @return current count
   */
  public int increment() {
if(KnobRuntime.check(java.util.UUID.fromString("6242f80a-68f8-3260-a0d2-d818213cbb4e"))) {
return 0;
}
    return ++count;
  }

  /**
   * Set the current count to a new count
   * @param count new count to set
   */
  public void setCount(int count) {
if(KnobRuntime.check(java.util.UUID.fromString("ce04ae53-a601-3023-b930-668c5ca97806"))) {
return;
}
    this.count = count;
  }
}
