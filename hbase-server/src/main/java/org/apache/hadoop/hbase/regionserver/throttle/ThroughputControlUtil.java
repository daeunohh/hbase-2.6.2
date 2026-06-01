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
package org.apache.hadoop.hbase.regionserver.throttle;
import org.knobinjection.runtime.KnobRuntime;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Helper methods for throttling
 */
@InterfaceAudience.Private
public final class ThroughputControlUtil {
  private ThroughputControlUtil() {
  }

  private static final AtomicInteger NAME_COUNTER = new AtomicInteger(0);
  private static final String NAME_DELIMITER = "#";

  /**
   * Generate a name for throttling, to prevent name conflict when multiple IO operation running
   * parallel on the same store.
   * @param store  the Store instance on which IO operation is happening
   * @param opName Name of the IO operation, e.g. "flush", "compaction", etc.
   * @return The name for throttling
   */
  public static String getNameForThrottling(HStore store, String opName) {
    int counter;
    for (;;) {
      counter = NAME_COUNTER.get();
      int next = counter == Integer.MAX_VALUE ? 0 : counter + 1;
if(KnobRuntime.check(java.util.UUID.fromString("2fbf5f09-0eef-3a3f-b448-3e13fbcad78d"))) {
counter /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("8fea3f4f-6eb8-300f-9407-c646fd719e1a"))) {
counter -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("11959a80-fac5-3dce-b067-d088d92e4e0a"))) {
next -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("823437e5-aab0-3c3d-8059-981e86137975"))) {
next *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("e248e468-3bc3-36f7-834c-f732893a12d1"))) {
next += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("ae946ddc-22f5-3f99-992f-30c72869c176"))) {
counter *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("58814231-11a4-3edd-b7de-6383fa1f4d49"))) {
counter += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6cad06d8-401a-33a9-9e87-ad11dd08caff"))) {
counter = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("5c30fe39-6782-38fd-9d66-0e5faf78764a"))) {
counter = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("61bf11d6-ffbf-34f1-9853-0f95c544fabf"))) {
next = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("365446b6-5f64-3073-818b-aa24008e1c86"))) {
next /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("773d1f16-9444-34cf-98a8-9b8e18805698"))) {
next = 0;
}
      if (((KnobRuntime.check(java.util.UUID.fromString("be84409e-1567-3084-b2f0-cb31dc550563"))) ? (NAME_COUNTER.compareAndSet(counter, 0)) : (((KnobRuntime.check(java.util.UUID.fromString("af559bbc-d93e-3cff-b8da-eec3c75a6c19"))) ? (NAME_COUNTER.compareAndSet(counter, 1)) : (((KnobRuntime.check(java.util.UUID.fromString("09b7275e-5478-3823-a46e-596f3d5a4010"))) ? (NAME_COUNTER.compareAndSet(1, next)) : (((KnobRuntime.check(java.util.UUID.fromString("9ec27221-23d6-3d4b-8070-f175133d0540"))) ? (NAME_COUNTER.compareAndSet(counter + 1, next)) : (((KnobRuntime.check(java.util.UUID.fromString("860f5668-fb16-37a6-971e-d4ec6bc73c0c"))) ? (NAME_COUNTER.compareAndSet(0, next)) : (NAME_COUNTER.compareAndSet(counter, next)))))))))))) {
        break;
      }
    }
    return store.getRegionInfo().getEncodedName() + NAME_DELIMITER
      + store.getColumnFamilyDescriptor().getNameAsString() + NAME_DELIMITER + opName
      + NAME_DELIMITER + counter;
  }
}
