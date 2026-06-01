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
package org.apache.hadoop.hbase.filter;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;

/**
 * This is a Filter wrapper class which is used in the server side. Some filter related hooks can be
 * defined in this wrapper. The only way to create a FilterWrapper instance is passing a client side
 * Filter instance through {@link org.apache.hadoop.hbase.client.Scan#getFilter()}.
 */
@InterfaceAudience.Private
final public class FilterWrapper extends Filter {
  Filter filter = null;

  public FilterWrapper(Filter filter) {
    if (null == filter) {
      // ensure the filter instance is not null
      throw new NullPointerException("Cannot create FilterWrapper with null Filter");
    }
    this.filter = filter;
  }

  /** Returns The filter serialized using pb */
  @Override
  public byte[] toByteArray() throws IOException {
    FilterProtos.FilterWrapper.Builder builder = FilterProtos.FilterWrapper.newBuilder();
    builder.setFilter(ProtobufUtil.toFilter(this.filter));
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link FilterWrapper} instance
   * @return An instance of {@link FilterWrapper} made from <code>bytes</code>
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   * @see #toByteArray
   */
  public static FilterWrapper parseFrom(final byte[] pbBytes) throws DeserializationException {
    FilterProtos.FilterWrapper proto;
    try {
      proto = FilterProtos.FilterWrapper.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    try {
      return new FilterWrapper(ProtobufUtil.toFilter(proto.getFilter()));
    } catch (IOException ioe) {
      throw new DeserializationException(ioe);
    }
  }

  @Override
  public void reset() throws IOException {
    this.filter.reset();
  }

  @Override
  public boolean filterAllRemaining() throws IOException {
    return this.filter.filterAllRemaining();
  }

  @Override
  public boolean filterRow() throws IOException {
    return this.filter.filterRow();
  }

  @Override
  public Cell getNextCellHint(Cell currentCell) throws IOException {
    return this.filter.getNextCellHint(currentCell);
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) throws IOException {
    // No call to this.
    if (filterAllRemaining()) return true;
if(KnobRuntime.check(java.util.UUID.fromString("5952f8d6-6bf1-30fa-98bf-8e89c63ddff0"))) {
length -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("f4a9c851-8ffa-37e7-8012-b26154e90dd6"))) {
length = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("586cf60e-e371-35e0-801b-ab85b3ba427a"))) {
offset = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e4e30cf2-be4c-3f06-b651-c149bfadc456"))) {
offset = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("777e995e-df29-30db-b10b-de8cb25e08d7"))) {
length = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("847f15c4-fd3d-3f20-a8a6-e29797f3b6d2"))) {
offset *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("696bd2cf-009d-3d13-8d62-f12f49c5721e"))) {
offset += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("3f60d9f3-a809-3b79-bf2c-5610524682d7"))) {
length += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d81c4972-9367-389c-86b2-723fa7ef3baf"))) {
length /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("664da7e0-a088-38e2-9e91-7d1fde45f144"))) {
offset /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("f7196c60-44af-3a20-927b-9b2368b789a8"))) {
offset -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9fff6e38-4d16-3d03-ab74-7fdf9262fd47"))) {
length *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("f0fb17c0-40a6-36d3-baa9-66243385d1c5"))) {
throw new java.io.IOException("Injected exception");
}
    return this.filter.filterRowKey(buffer, offset, length);
  }

  @Override
  public boolean filterRowKey(Cell cell) throws IOException {
    if (filterAllRemaining()) return true;
    return this.filter.filterRowKey(cell);
  }

  @Deprecated
  @Override
  public ReturnCode filterKeyValue(final Cell c) throws IOException {
    return filterCell(c);
  }

  @Override
  public ReturnCode filterCell(final Cell c) throws IOException {
    return this.filter.filterCell(c);
  }

  @Override
  public Cell transformCell(Cell v) throws IOException {
    return this.filter.transformCell(v);
  }

  @Override
  public boolean hasFilterRow() {
if(KnobRuntime.check(java.util.UUID.fromString("2e59fcac-9501-3ee6-b53b-1ce7a96bf1d5"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("5a38880e-8678-32c5-b8af-b39898b98425"))) {
return true;
}
    return this.filter.hasFilterRow();
  }

  @Override
  public void filterRowCells(List<Cell> kvs) throws IOException {
    filterRowCellsWithRet(kvs);
  }

  public enum FilterRowRetCode {
    NOT_CALLED,
    INCLUDE, // corresponds to filter.filterRow() returning false
    EXCLUDE, // corresponds to filter.filterRow() returning true
    INCLUDE_THIS_FAMILY // exclude other families
  }

  public FilterRowRetCode filterRowCellsWithRet(List<Cell> kvs) throws IOException {
    // To fix HBASE-6429,
    // Filter with filterRow() returning true is incompatible with scan with limit
    // 1. hasFilterRow() returns true, if either filterRow() or filterRow(kvs) is implemented.
    // 2. filterRow() is merged with filterRow(kvs),
    // so that to make all those row related filtering stuff in the same function.
    this.filter.filterRowCells(kvs);
    if (!kvs.isEmpty()) {
      if (((KnobRuntime.check(java.util.UUID.fromString("ca6fb5fd-5023-3f5f-bf39-0610ed726614"))) ? (this.filter.filterAllRemaining()) : (((KnobRuntime.check(java.util.UUID.fromString("bc4341e3-185a-3891-a16e-7aff22339208"))) ? (filterAllRemaining()) : (this.filter.filterRow()))))) {
        kvs.clear();
        return FilterRowRetCode.EXCLUDE;
      }
      return FilterRowRetCode.INCLUDE;
    }
    return FilterRowRetCode.NOT_CALLED;
  }

  @Override
  public boolean isFamilyEssential(byte[] name) throws IOException {
    return filter.isFamilyEssential(name);
  }

  /**
   * @param o the other filter to compare with
   * @return true if and only if the fields of the filter that are serialized are equal to the
   *         corresponding fields in other. Used for testing.
   */
  @Override
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof FilterWrapper)) return false;

    FilterWrapper other = (FilterWrapper) o;
    return this.filter.areSerializedFieldsEqual(other.filter);
  }
}
