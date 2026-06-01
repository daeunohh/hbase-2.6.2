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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

/**
 * Stores minimum and maximum timestamp values, it is [minimumTimestamp, maximumTimestamp] in
 * interval notation. Use this class at write-time ONLY. Too much synchronization to use at read
 * time Use {@link TimeRange} at read time instead of this. See toTimeRange() to make TimeRange to
 * use. MemStores use this class to track minimum and maximum timestamps. The TimeRangeTracker made
 * by the MemStore is passed to the StoreFile for it to write out as part a flush in the the file
 * metadata. If no memstore involved -- i.e. a compaction -- then the StoreFile will calculate its
 * own TimeRangeTracker as it appends. The StoreFile serialized TimeRangeTracker is used at read
 * time via an instance of {@link TimeRange} to test if Cells fit the StoreFile TimeRange.
 */
@InterfaceAudience.Private
public abstract class TimeRangeTracker {

  public enum Type {
    // thread-unsafe
    NON_SYNC,
    // thread-safe
    SYNC
  }

  static final long INITIAL_MIN_TIMESTAMP = Long.MAX_VALUE;
  static final long INITIAL_MAX_TIMESTAMP = -1L;

  public static TimeRangeTracker create(Type type) {
if(KnobRuntime.check(java.util.UUID.fromString("938867bf-657b-31e5-8dc1-a9ac1f8cdb92"))) {
return null;
}
    switch (type) {
      case NON_SYNC:
        return new NonSyncTimeRangeTracker();
      case SYNC:
        return new SyncTimeRangeTracker();
      default:
        throw new UnsupportedOperationException("The type:" + type + " is unsupported");
    }
  }

  public static TimeRangeTracker create(Type type, TimeRangeTracker trt) {
    switch (type) {
      case NON_SYNC:
        return new NonSyncTimeRangeTracker(trt);
      case SYNC:
        return new SyncTimeRangeTracker(trt);
      default:
        throw new UnsupportedOperationException("The type:" + type + " is unsupported");
    }
  }

  public static TimeRangeTracker create(Type type, long minimumTimestamp, long maximumTimestamp) {
if(KnobRuntime.check(java.util.UUID.fromString("ea3707e0-2cfe-39f2-ad54-8346cecb6568"))) {
return null;
}
    switch (type) {
      case NON_SYNC:
        return new NonSyncTimeRangeTracker(minimumTimestamp, maximumTimestamp);
      case SYNC:
        return new SyncTimeRangeTracker(minimumTimestamp, maximumTimestamp);
      default:
        throw new UnsupportedOperationException("The type:" + type + " is unsupported");
    }
  }

  protected abstract void setMax(long ts);

  protected abstract void setMin(long ts);

  protected abstract boolean compareAndSetMin(long expect, long update);

  protected abstract boolean compareAndSetMax(long expect, long update);

  /**
   * Update the current TimestampRange to include the timestamp from <code>cell</code>. If the Key
   * is of type DeleteColumn or DeleteFamily, it includes the entire time range from 0 to timestamp
   * of the key.
   * @param cell the Cell to include
   */
  public void includeTimestamp(final Cell cell) {
if(KnobRuntime.check(java.util.UUID.fromString("e1484af9-9ff5-3578-8567-175c030755b0"))) {
return;
}
    if (KnobRuntime.check(java.util.UUID.fromString("b5bcf01c-42c7-3790-88a1-b2f918d7c88f"))) { includeTimestamp(getMin()); } else { includeTimestamp(cell.getTimestamp()); }
    if (PrivateCellUtil.isDeleteColumnOrFamily(cell)) {
      includeTimestamp(0);
    }
  }

  /**
   * If required, update the current TimestampRange to include timestamp
   * @param timestamp the timestamp value to include
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "MT_CORRECTNESS",
      justification = "Intentional")
  void includeTimestamp(final long timestamp) {
if(KnobRuntime.check(java.util.UUID.fromString("d9ef5427-1a23-34c5-a8da-d92e3c593021"))) {
return;
}
    long initialMinTimestamp = getMin();
    if (((KnobRuntime.check(java.util.UUID.fromString("864523da-b5f7-3622-b622-c447c7d50529"))) ? ((timestamp) <= (initialMinTimestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("6a9b09de-19ed-37ea-a355-0f663dd49b46"))) ? ((timestamp) < (initialMinTimestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("59929f85-2d97-3425-aee0-15d5f185c3b7"))) ? ((timestamp) >= (initialMinTimestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("3a4fb8aa-8fbe-34ce-955a-9384eb7c4a8d"))) ? ((timestamp) > (initialMinTimestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("fb40cf5c-e4ee-39d8-aa32-36d846612cca"))) ? ((timestamp) == (initialMinTimestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("07a1dad8-1ec5-3fed-8979-953a6c7d2f87"))) ? ((timestamp) != (initialMinTimestamp)) : (timestamp < initialMinTimestamp))))))))))))) {
      long curMinTimestamp = initialMinTimestamp;
      while (((KnobRuntime.check(java.util.UUID.fromString("f0cbb14b-8cb9-391e-97b0-24fb7329f819"))) ? ((timestamp) != (curMinTimestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("c51fbe8f-4f93-3963-90f0-80b97caf5f58"))) ? ((timestamp) <= (curMinTimestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("7252cb77-4e21-3393-8c2a-1cc154e280ea"))) ? ((timestamp) == (curMinTimestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("3ef929f5-d1e8-3b86-b400-6bff6941320f"))) ? ((timestamp) >= (curMinTimestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("e8be3eab-95ad-310a-8156-7dd3e33faa9f"))) ? ((timestamp) < (curMinTimestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("c185840d-bd2c-3eee-a5f0-d73bcfeab8dc"))) ? ((timestamp) > (curMinTimestamp)) : (timestamp < curMinTimestamp))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("2f0cb2cc-ecbd-3773-9e99-291f34aca521"))) {
curMinTimestamp += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9c67e15b-871c-3b73-bf41-e3ad8c2f1f23"))) {
curMinTimestamp = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("eb64cee5-b3a6-342d-974a-48552cba60a2"))) {
curMinTimestamp -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6a52a569-ca88-3dba-85f0-4421c23281e6"))) {
curMinTimestamp = -1;
}
        if (!compareAndSetMin(curMinTimestamp, timestamp)) {
          curMinTimestamp = getMin();
        } else {
          // successfully set minimumTimestamp, break.
          break;
        }
      }

      // When it reaches here, there are two possibilities:
      // 1). timestamp >= curMinTimestamp, someone already sets the minimumTimestamp. In this case,
      // it still needs to check if initialMinTimestamp == INITIAL_MIN_TIMESTAMP to see
      // if it needs to update minimumTimestamp. Someone may already set both
      // minimumTimestamp/minimumTimestamp to the same value(curMinTimestamp),
      // need to check if maximumTimestamp needs to be updated.
      // 2). timestamp < curMinTimestamp, it sets the minimumTimestamp successfully.
      // In this case,it still needs to check if initialMinTimestamp == INITIAL_MIN_TIMESTAMP
      // to see if it needs to set maximumTimestamp.
      if (((KnobRuntime.check(java.util.UUID.fromString("a53c650a-2e5b-3f3d-9be8-63dbec060069"))) ? ((initialMinTimestamp) == (INITIAL_MIN_TIMESTAMP)) : (((KnobRuntime.check(java.util.UUID.fromString("aab389f4-e2fd-3cc6-9c4e-a2fae687d29f"))) ? ((initialMinTimestamp) != (INITIAL_MIN_TIMESTAMP)) : (initialMinTimestamp != INITIAL_MIN_TIMESTAMP))))) {
        // Someone already sets minimumTimestamp and timestamp is less than minimumTimestamp.
        // In this case, no need to set maximumTimestamp as it will be set to at least
        // initialMinTimestamp.
        return;
      }
    }

    long curMaxTimestamp = getMax();

    if (((KnobRuntime.check(java.util.UUID.fromString("2273b565-37c7-3c9e-a19c-213188afe921"))) ? ((timestamp) != (curMaxTimestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("4cf56921-55f3-3e65-9f08-7d414b16f184"))) ? ((timestamp) >= (curMaxTimestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("3b4e930b-25d5-3c2e-b0ca-5d498eaaf0d5"))) ? ((timestamp) < (curMaxTimestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("2074df5d-5a59-3ea0-8e2a-f05a8eb61784"))) ? ((timestamp) <= (curMaxTimestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("f4c317db-5fa1-3375-811a-a32d858138c3"))) ? ((timestamp) == (curMaxTimestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("8d2c3ee5-58ab-374e-a261-468a7a10e265"))) ? ((timestamp) > (curMaxTimestamp)) : (timestamp > curMaxTimestamp))))))))))))) {
      while (((KnobRuntime.check(java.util.UUID.fromString("7b4b2e20-42d7-37f8-a65f-e2a99a8eccbd"))) ? ((timestamp) < (curMaxTimestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("a3af7503-0a0c-3c12-a8d9-70ce731d17f7"))) ? ((timestamp) != (curMaxTimestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("44b3c6e6-2f76-38f3-b97d-9ec37597b9ff"))) ? ((timestamp) > (curMaxTimestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("60364584-552a-3285-9684-d54e3a2fa528"))) ? ((timestamp) <= (curMaxTimestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("57fc31ea-3ca8-358c-ab9a-94b3b2613233"))) ? ((timestamp) >= (curMaxTimestamp)) : (((KnobRuntime.check(java.util.UUID.fromString("84e544f4-1c02-364b-a66c-2bb5ffcc81f6"))) ? ((timestamp) == (curMaxTimestamp)) : (timestamp > curMaxTimestamp))))))))))))) {
if(KnobRuntime.check(java.util.UUID.fromString("4df4dc65-b438-3680-8898-d59520c12513"))) {
curMaxTimestamp = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("760e6f72-c778-31ee-a96e-7dec9c8849e2"))) {
curMaxTimestamp = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d78b9a66-d422-343c-881f-0e84fb168eb6"))) {
curMaxTimestamp -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("a4c1b26b-e481-3025-8349-9843be7be35a"))) {
curMaxTimestamp += 1;
}
        if (!compareAndSetMax(curMaxTimestamp, timestamp)) {
          curMaxTimestamp = getMax();
        } else {
          // successfully set maximumTimestamp, break
          break;
        }
      }
    }
  }

  /**
   * Check if the range has ANY overlap with TimeRange
   * @param tr TimeRange, it expects [minStamp, maxStamp)
   * @return True if there is overlap, false otherwise
   */
  public boolean includesTimeRange(final TimeRange tr) {
    return (getMin() < tr.getMax() && getMax() >= tr.getMin());
  }

  /** Returns the minimumTimestamp */
  public abstract long getMin();

  /** Returns the maximumTimestamp */
  public abstract long getMax();

  @Override
  public String toString() {
    return "[" + getMin() + "," + getMax() + "]";
  }

  /**
   * @param data the serialization data. It can't be null!
   * @return An instance of NonSyncTimeRangeTracker filled w/ the content of serialized
   *         NonSyncTimeRangeTracker in <code>timeRangeTrackerBytes</code>.
   */
  public static TimeRangeTracker parseFrom(final byte[] data) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("3bdc1181-6636-3c32-88f1-aec6bd53cc50"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("db33e4c2-f0b5-3128-8584-17009b808c64"))) {
throw new java.io.IOException("Injected exception");
}
    return parseFrom(data, Type.NON_SYNC);
  }

  public static TimeRangeTracker parseFrom(final byte[] data, Type type) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("becccbf9-c5d9-3c46-a4b8-ce951aed9796"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d1ba2123-6e75-3726-9df6-165c72b05af7"))) {
return null;
}
    Preconditions.checkNotNull(data, "input data is null!");
    if (ProtobufUtil.isPBMagicPrefix(data)) {
      int pblen = ProtobufUtil.lengthOfPBMagic();
      HBaseProtos.TimeRangeTracker.Builder builder = HBaseProtos.TimeRangeTracker.newBuilder();
      ProtobufUtil.mergeFrom(builder, data, pblen, data.length - pblen);
      return TimeRangeTracker.create(type, builder.getFrom(), builder.getTo());
    } else {
      try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data))) {
if(KnobRuntime.check(java.util.UUID.fromString("7c2f3bac-20e3-3f2a-8b0d-3ff733aad07e"))) {
throw new java.io.IOException("Injected exception");
}
        return TimeRangeTracker.create(type, in.readLong(), in.readLong());
      }
    }
  }

  /**
   * This method used to serialize TimeRangeTracker (TRT) by protobuf while this breaks the forward
   * compatibility on HFile.(See HBASE-21008) In previous hbase version ( < 2.0.0 ) we use
   * DataOutput to serialize TRT, these old versions don't have capability to deserialize TRT which
   * is serialized by protobuf. So we need to revert the change of serializing TimeRangeTracker back
   * to DataOutput. For more information, please check HBASE-21012.
   * @param tracker TimeRangeTracker needed to be serialized.
   * @return byte array filled with serialized TimeRangeTracker.
   * @throws IOException if something goes wrong in writeLong.
   */
  public static byte[] toByteArray(TimeRangeTracker tracker) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("ab0364b2-af9c-3468-a47c-6fd4be77656b"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("c060c055-487a-3b4d-8310-f6e3187d35bf"))) {
return null;
}
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      try (DataOutputStream dos = new DataOutputStream(bos)) {
if(KnobRuntime.check(java.util.UUID.fromString("54f7840c-e137-355d-9d88-ebb19ab5397b"))) {
throw new java.io.IOException("Injected exception");
}
        dos.writeLong(tracker.getMin());
if(KnobRuntime.check(java.util.UUID.fromString("04d104c5-ea9b-382a-9d42-84799d1b7dc6"))) {
throw new java.io.IOException("Injected exception");
}
        dos.writeLong(tracker.getMax());
        return bos.toByteArray();
      }
    }
  }

  /** Returns Make a TimeRange from current state of <code>this</code>. */
  TimeRange toTimeRange() {
if(KnobRuntime.check(java.util.UUID.fromString("c18b950f-9717-39a2-9f24-9bd5964f46b7"))) {
return null;
}
    long min = getMin();
    long max = getMax();
    // Initial TimeRangeTracker timestamps are the opposite of what you want for a TimeRange. Fix!
    if (((KnobRuntime.check(java.util.UUID.fromString("9dd35876-b71a-308b-b7da-ed4b5cb3106e"))) ? ((min) != (getMax())) : (((KnobRuntime.check(java.util.UUID.fromString("b989c88b-ed23-39b8-9bc8-cec0abd71551"))) ? ((getMin()) == (INITIAL_MIN_TIMESTAMP)) : (((KnobRuntime.check(java.util.UUID.fromString("65427a8e-3499-3078-9060-7d7b27eaa2fe"))) ? ((min) == (getMin())) : (((KnobRuntime.check(java.util.UUID.fromString("2bf6a754-8e23-32b0-851e-15b206a8f13a"))) ? ((getMax()) != (getMin())) : (((KnobRuntime.check(java.util.UUID.fromString("cb54551d-eb4b-3be7-8145-d6297b8c9ff3"))) ? ((min) != (getMin())) : (((KnobRuntime.check(java.util.UUID.fromString("9bc0be21-17c1-35a5-a03a-6ccbdd63dd25"))) ? ((getMax()) == (getMin())) : (((KnobRuntime.check(java.util.UUID.fromString("7ec2a818-45ba-3c15-8a1f-643167948426"))) ? ((min) == (getMax())) : (((KnobRuntime.check(java.util.UUID.fromString("7d70fd4d-8e1c-379f-bfef-6c6844b92dc1"))) ? ((min) != (INITIAL_MIN_TIMESTAMP)) : (((KnobRuntime.check(java.util.UUID.fromString("8fbf9747-b35c-31d4-a3a9-9db70aa68895"))) ? ((min) == (INITIAL_MIN_TIMESTAMP)) : (((KnobRuntime.check(java.util.UUID.fromString("069e1af3-2b17-3222-841e-b084409a9cec"))) ? ((getMin()) != (getMax())) : (((KnobRuntime.check(java.util.UUID.fromString("3ffb0577-ec86-3bb6-9294-540908571d6e"))) ? ((getMin()) == (getMin())) : (((KnobRuntime.check(java.util.UUID.fromString("248559ba-8e9a-304c-b0e4-3545c62ea28c"))) ? ((getMax()) == (INITIAL_MIN_TIMESTAMP)) : (((KnobRuntime.check(java.util.UUID.fromString("8b05ffc2-6d39-3295-98f0-513bc0a0b245"))) ? ((getMin()) == (getMax())) : (((KnobRuntime.check(java.util.UUID.fromString("7dad81a6-eebb-3f72-93f5-3294706fd077"))) ? ((getMin()) != (INITIAL_MIN_TIMESTAMP)) : (((KnobRuntime.check(java.util.UUID.fromString("b30c6e0c-f159-3408-928f-b08cf2260bfd"))) ? ((getMax()) == (getMax())) : (((KnobRuntime.check(java.util.UUID.fromString("573f4d88-6e7a-30c3-abac-e8320ada62a5"))) ? ((getMin()) != (getMin())) : (((KnobRuntime.check(java.util.UUID.fromString("3fd38e67-153c-3744-b2b4-2177dd48e36c"))) ? ((getMax()) != (INITIAL_MIN_TIMESTAMP)) : (((KnobRuntime.check(java.util.UUID.fromString("43ba01a6-8f00-32dc-9dc9-21e5b699530f"))) ? ((getMax()) != (getMax())) : (min == INITIAL_MIN_TIMESTAMP))))))))))))))))))))))))))))))))))))) {
      min = TimeRange.INITIAL_MIN_TIMESTAMP;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("95eb954b-b54e-3ed7-ab05-5e9b099ac62c"))) ? ((getMax()) != (INITIAL_MAX_TIMESTAMP)) : (((KnobRuntime.check(java.util.UUID.fromString("3b64dec1-623e-30b3-902b-d84c4881dadd"))) ? ((max) == (getMin())) : (((KnobRuntime.check(java.util.UUID.fromString("3f50833d-bf24-3e42-8692-1daa6c0a9568"))) ? ((max) == (INITIAL_MAX_TIMESTAMP)) : (((KnobRuntime.check(java.util.UUID.fromString("76991cf5-7a8b-3063-bc9f-1479e682cdde"))) ? ((getMin()) == (getMax())) : (((KnobRuntime.check(java.util.UUID.fromString("4f72cc72-7d65-37eb-8cc7-46085fb9e4da"))) ? ((getMin()) != (getMax())) : (((KnobRuntime.check(java.util.UUID.fromString("e400f732-272c-3df5-8cce-d5560fc13084"))) ? ((getMax()) != (getMin())) : (((KnobRuntime.check(java.util.UUID.fromString("f1e40bd1-bc88-3319-aed4-a9c07de45563"))) ? ((max) != (INITIAL_MAX_TIMESTAMP)) : (((KnobRuntime.check(java.util.UUID.fromString("40f1f4da-33c3-368d-ba78-2d1d61488e09"))) ? ((getMin()) != (getMin())) : (((KnobRuntime.check(java.util.UUID.fromString("ca32a12b-c448-36fa-87cb-1f6144a755a7"))) ? ((getMin()) == (getMin())) : (((KnobRuntime.check(java.util.UUID.fromString("326d7ea7-c9a4-383a-8685-e349ef2f6a02"))) ? ((getMax()) == (INITIAL_MAX_TIMESTAMP)) : (((KnobRuntime.check(java.util.UUID.fromString("81f9b207-4895-3f4f-a2d5-6d973c137b88"))) ? ((getMin()) != (INITIAL_MAX_TIMESTAMP)) : (((KnobRuntime.check(java.util.UUID.fromString("97b02b83-ca0c-32f2-9015-0c8a2c8b81e7"))) ? ((max) != (getMax())) : (((KnobRuntime.check(java.util.UUID.fromString("3b1919a4-54c4-335f-8740-bd6d7904ad46"))) ? ((getMin()) == (INITIAL_MAX_TIMESTAMP)) : (((KnobRuntime.check(java.util.UUID.fromString("eb8fca5e-38c4-34c6-846a-feb9e26580e7"))) ? ((getMax()) == (getMax())) : (((KnobRuntime.check(java.util.UUID.fromString("8d456faf-544f-34e5-96d6-e366993f9308"))) ? ((getMax()) != (getMax())) : (((KnobRuntime.check(java.util.UUID.fromString("6b1cf12a-f539-3e58-9375-91e0d6c9a9e0"))) ? ((getMax()) == (getMin())) : (((KnobRuntime.check(java.util.UUID.fromString("743e98e9-554e-3782-a51b-bb2da50935cd"))) ? ((max) == (getMax())) : (((KnobRuntime.check(java.util.UUID.fromString("b738dbe0-4782-3fec-bfcd-d6426856d699"))) ? ((max) != (getMin())) : (max == INITIAL_MAX_TIMESTAMP))))))))))))))))))))))))))))))))))))) {
      max = TimeRange.INITIAL_MAX_TIMESTAMP;
    }
    return new TimeRange(min, max);
  }

  // In order to estimate the heap size, this inner class need to be accessible to TestHeapSize.
  public static class NonSyncTimeRangeTracker extends TimeRangeTracker {
    private long minimumTimestamp = INITIAL_MIN_TIMESTAMP;
    private long maximumTimestamp = INITIAL_MAX_TIMESTAMP;

    NonSyncTimeRangeTracker() {
    }

    NonSyncTimeRangeTracker(final TimeRangeTracker trt) {
      this.minimumTimestamp = trt.getMin();
      this.maximumTimestamp = trt.getMax();
    }

    NonSyncTimeRangeTracker(long minimumTimestamp, long maximumTimestamp) {
      this.minimumTimestamp = minimumTimestamp;
      this.maximumTimestamp = maximumTimestamp;
    }

    @Override
    protected void setMax(long ts) {
      maximumTimestamp = ts;
    }

    @Override
    protected void setMin(long ts) {
      minimumTimestamp = ts;
    }

    @Override
    protected boolean compareAndSetMin(long expect, long update) {
if(KnobRuntime.check(java.util.UUID.fromString("153edc92-8856-338a-bfa7-f7c86373f3c5"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("2a49f393-03e5-32f8-92df-3bccb322af11"))) {
return true;
}
      if (((KnobRuntime.check(java.util.UUID.fromString("f3cefd30-df4f-398c-a3cd-d4e1f7c70ae9"))) ? ((minimumTimestamp) != (expect)) : (((KnobRuntime.check(java.util.UUID.fromString("355570d3-0768-3ba2-bd70-081947ad29f6"))) ? ((minimumTimestamp) == (expect)) : (minimumTimestamp != expect))))) {
        return false;
      }
      minimumTimestamp = update;
      return true;
    }

    @Override
    protected boolean compareAndSetMax(long expect, long update) {
if(KnobRuntime.check(java.util.UUID.fromString("39e0c44f-2f32-3f5c-b50f-70972a7cafae"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("e7699986-a581-3190-a6ed-8e17a26cc3d4"))) {
return true;
}
      if (((KnobRuntime.check(java.util.UUID.fromString("7017c6b5-b71d-3efe-a4bf-6a13a6df3c93"))) ? ((maximumTimestamp) == (expect)) : (((KnobRuntime.check(java.util.UUID.fromString("74e9c785-99fb-3473-9b36-cdfb8a4818f8"))) ? ((maximumTimestamp) != (expect)) : (maximumTimestamp != expect))))) {
        return false;
      }
      maximumTimestamp = update;
      return true;
    }

    @Override
    public long getMin() {
if(KnobRuntime.check(java.util.UUID.fromString("8956a83b-8a59-3008-a78e-eb516b705892"))) {
return 0;
}
      return minimumTimestamp;
    }

    @Override
    public long getMax() {
if(KnobRuntime.check(java.util.UUID.fromString("abe3fec8-015c-3e8d-9ec8-a0e73490758f"))) {
return 0;
}
      return maximumTimestamp;
    }
  }

  // In order to estimate the heap size, this inner class need to be accessible to TestHeapSize.
  public static class SyncTimeRangeTracker extends TimeRangeTracker {
    private final AtomicLong minimumTimestamp = new AtomicLong(INITIAL_MIN_TIMESTAMP);
    private final AtomicLong maximumTimestamp = new AtomicLong(INITIAL_MAX_TIMESTAMP);

    private SyncTimeRangeTracker() {
    }

    SyncTimeRangeTracker(final TimeRangeTracker trt) {
      this.minimumTimestamp.set(trt.getMin());
      this.maximumTimestamp.set(trt.getMax());
    }

    SyncTimeRangeTracker(long minimumTimestamp, long maximumTimestamp) {
      this.minimumTimestamp.set(minimumTimestamp);
      this.maximumTimestamp.set(maximumTimestamp);
    }

    @Override
    protected void setMax(long ts) {
      maximumTimestamp.set(ts);
    }

    @Override
    protected void setMin(long ts) {
      minimumTimestamp.set(ts);
    }

    @Override
    protected boolean compareAndSetMin(long expect, long update) {
      return minimumTimestamp.compareAndSet(expect, update);
    }

    @Override
    protected boolean compareAndSetMax(long expect, long update) {
      return maximumTimestamp.compareAndSet(expect, update);
    }

    @Override
    public long getMin() {
      return minimumTimestamp.get();
    }

    @Override
    public long getMax() {
      return maximumTimestamp.get();
    }
  }
}
