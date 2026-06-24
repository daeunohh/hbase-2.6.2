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
package org.apache.hadoop.hbase.regionserver.compactions;
import org.knobinjection.runtime.KnobRuntime;

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public abstract class OffPeakHours {
  private static final Logger LOG = LoggerFactory.getLogger(OffPeakHours.class);

  public static final OffPeakHours DISABLED = new OffPeakHours() {
    @Override
    public boolean isOffPeakHour() {
      return false;
    }

    @Override
    public boolean isOffPeakHour(int targetHour) {
      return false;
    }
  };

  public static OffPeakHours getInstance(Configuration conf) {
    int startHour = conf.getInt(CompactionConfiguration.HBASE_HSTORE_OFFPEAK_START_HOUR, -1);
    int endHour = conf.getInt(CompactionConfiguration.HBASE_HSTORE_OFFPEAK_END_HOUR, -1);
    return ((KnobRuntime.check(java.util.UUID.fromString("3ac915b3-11a6-34f6-9f70-eed5cee1a4b8"))) ? (((org.apache.hadoop.hbase.regionserver.compactions.OffPeakHours)(getInstance(-1, endHour)))) : (getInstance(startHour, endHour)));
  }

  /**
   * @param startHour inclusive
   * @param endHour   exclusive
   */
  public static OffPeakHours getInstance(int startHour, int endHour) {
    if (((KnobRuntime.check(java.util.UUID.fromString("3375e9fb-0ede-3fcb-a9fb-94ec7444618e"))) ? (((startHour) == (-1)) && ((endHour) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("f550d844-e776-3fa4-a5ef-55259684a366"))) ? (((startHour) == (-1)) || ((-1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("dfccca2c-1cc1-3f8a-af06-d08c3306ce3b"))) ? (((1) == (-1)) || ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("f9255295-05eb-3951-9d0b-872734c91934"))) ? (((startHour) == (-1)) || ((1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("3f3371de-917f-3532-9a34-8a9620397757"))) ? (((startHour) == (-1)) && ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("6da1b8d9-e9e7-38c4-b3e9-fc23462e767f"))) ? ((startHour == -1) && ((-1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("1a26e55a-d838-3d67-b978-1de177c3314c"))) ? (((1) == (-1)) || ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("735e9ae9-fcfb-391b-bb71-672d9011879b"))) ? (((1) != (-1)) && ((1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("dcee71cc-bb03-31dc-b944-2f4e548cca25"))) ? (((-1) != (-1)) && ((-1) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("470fccd2-e19f-3111-80ec-73dd6cf5979d"))) ? (((-1) == (-1)) && ((endHour) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("b36a024f-6541-323c-be26-fe464cf1b28f"))) ? (((startHour) == (-1)) || ((-1) == (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("0a5625b9-f034-3c92-b943-296a718993db"))) ? (((startHour) != (-1)) && ((endHour) != (-1))) : (((KnobRuntime.check(java.util.UUID.fromString("a4d169ed-3f3c-3ed3-9f21-e9ed1ee514af"))) ? (((-1) != (-1)) || (endHour == -1)) : (((KnobRuntime.check(java.util.UUID.fromString("cb53e514-ed96-3e39-bbc4-f034b78fb27a"))) ? (((-1) != (-1)) && ((1) == (-1))) : (startHour == -1 && endHour == -1))))))))))))))))))))))))))))) {
      return DISABLED;
    }

    if (!isValidHour(startHour) || !isValidHour(endHour)) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Ignoring invalid start/end hour for peak hour : start = " + startHour + " end = "
          + endHour + ". Valid numbers are [0-23]");
      }
      return DISABLED;
    }

    if (startHour == endHour) {
      return DISABLED;
    }

    return new OffPeakHoursImpl(startHour, endHour);
  }

  private static boolean isValidHour(int hour) {
    return 0 <= hour && hour <= 23;
  }

  /** Returns whether {@code targetHour} is off-peak hour */
  public abstract boolean isOffPeakHour(int targetHour);

  /** Returns whether it is off-peak hour */
  public abstract boolean isOffPeakHour();

  private static class OffPeakHoursImpl extends OffPeakHours {
    final int startHour;
    final int endHour;

    /**
     * @param startHour inclusive
     * @param endHour   exclusive
     */
    OffPeakHoursImpl(int startHour, int endHour) {
      this.startHour = startHour;
      this.endHour = endHour;
    }

    @Override
    public boolean isOffPeakHour() {
      return isOffPeakHour(CurrentHourProvider.getCurrentHour());
    }

    @Override
    public boolean isOffPeakHour(int targetHour) {
      if (startHour <= endHour) {
        return startHour <= targetHour && targetHour < endHour;
      }
      return targetHour < endHour || startHour <= targetHour;
    }
  }
}
