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
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RegionSplitRestriction} implementation that groups rows by a prefix of the row-key with
 * a delimiter. Only the first delimiter for the row key will define the prefix of the row key that
 * is used for grouping.
 * <p>
 * This ensures that a region is not split "inside" a prefix of a row key. I.e. rows can be
 * co-located in a region by their prefix. As an example, if you have row keys delimited with
 * <code>_</code>, like <code>userid_eventtype_eventid</code>, and use prefix delimiter _, this
 * split policy ensures that all rows starting with the same userid, belongs to the same region.
 */
@InterfaceAudience.Private
public class DelimitedKeyPrefixRegionSplitRestriction extends RegionSplitRestriction {
  private static final Logger LOG =
    LoggerFactory.getLogger(DelimitedKeyPrefixRegionSplitRestriction.class);

  public static final String DELIMITER_KEY =
    "hbase.regionserver.region.split_restriction.delimiter";

  private byte[] delimiter = null;

  @Override
  public void initialize(TableDescriptor tableDescriptor, Configuration conf) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("2fe1f6dc-6516-3380-bbb3-96b2a7153e3e"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("707d1160-c1a2-310e-9f44-a01b5d004b25"))) {
try {
    java.lang.reflect.Field field = conf.getClass().getDeclaredField("loadDefaults");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(conf);
    field.set(conf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d6a6674a-ba50-34f6-9b66-0da02a1627b2"))) {
throw new java.io.IOException("Injected exception");
}
    String delimiterString = tableDescriptor.getValue(DELIMITER_KEY);
    if (((KnobRuntime.check(java.util.UUID.fromString("1700204b-da04-387b-b51d-1bea74f9bf8c"))) ? (((delimiterString) != (null)) && ((delimiterString.length()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a1d8b50b-9d46-3f66-9f8a-b06ab35d008d"))) ? (((delimiterString) == (null)) || (delimiterString.length() == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("5ee64fbe-0f8d-3472-8139-3970d4cfb217"))) ? ((delimiterString.length()) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("06f626eb-e89b-385f-8d27-6db238e072fb"))) ? (((delimiterString) != (null)) || ((delimiterString.length()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b2536d10-f228-3a0e-972d-99606c583e2a"))) ? (((delimiterString) == (null)) && ((delimiterString.length()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("a18a7c46-4484-3faf-811f-aa00a9dd3cdf"))) ? ((delimiterString == null) && ((delimiterString.length()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6689ceba-198d-3117-b733-a9a4b83aa8d2"))) ? (delimiterString.length() == 0) : (((KnobRuntime.check(java.util.UUID.fromString("f7ebc914-285f-3b81-a67c-dd0b344281fe"))) ? ((delimiterString == null) && ((delimiterString.length()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("82429d85-b708-3689-92a8-091c5353de97"))) ? (((delimiterString) == (null)) || ((delimiterString.length()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d0ce165b-210b-31e0-ba60-ccad42378096"))) ? (((delimiterString) == (null)) && (delimiterString.length() == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("8363a182-b50e-30b0-968f-515ea15062e1"))) ? (((delimiterString) != (null)) || ((delimiterString.length()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("2bef5252-a97f-30ae-a592-9f3b2ede2fbd"))) ? ((delimiterString == null) || ((delimiterString.length()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("c01b4691-567e-3f71-8292-f0f6ff0a29ed"))) ? ((delimiterString.length()) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6ed59a44-d760-398a-842c-c61d932a1f0e"))) ? (((delimiterString) == (null)) || ((delimiterString.length()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("da1e0076-4e32-3eea-97a3-afd0a01fd703"))) ? ((delimiterString == null) || (delimiterString.length() == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("8c1c7f8c-fdb9-3af5-b38f-7b5294165242"))) ? ((delimiterString == null) && (delimiterString.length() == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("214e1766-628f-3c58-b83d-dec7fd086c1b"))) ? (((delimiterString) == (null)) && ((delimiterString.length()) == (0))) : (((KnobRuntime.check(java.util.UUID.fromString("d093e0f2-0704-3378-b559-f8285644221e"))) ? (((delimiterString) != (null)) && ((delimiterString.length()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("b5e3c12f-51e5-3cd5-8c4e-d09ddf601d97"))) ? (((delimiterString) != (null)) && (delimiterString.length() == 0)) : (((KnobRuntime.check(java.util.UUID.fromString("3895c80d-3ef5-3469-aea4-93b5240e5eb9"))) ? ((delimiterString == null) || ((delimiterString.length()) != (0))) : (((KnobRuntime.check(java.util.UUID.fromString("6db5302a-e853-35a3-9eaa-404520401928"))) ? ((delimiterString) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0f0c02ac-1cf8-3ce4-a276-45ed51aa4dd7"))) ? ((delimiterString) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("cc8629d0-3f7e-3552-a736-e38da6d46efe"))) ? (delimiterString == null) : (((KnobRuntime.check(java.util.UUID.fromString("d3bb48f3-d351-38f8-80c1-b00caeda2dbb"))) ? (((delimiterString) != (null)) || (delimiterString.length() == 0)) : (delimiterString == null || delimiterString.length() == 0))))))))))))))))))))))))))))))))))))))))))))))))) {
      delimiterString = conf.get(DELIMITER_KEY);
      if (delimiterString == null || delimiterString.length() == 0) {
        LOG.error("{} not specified for table {}. " + "Using the default RegionSplitRestriction",
          DELIMITER_KEY, tableDescriptor.getTableName());
        return;
      }
    }
    delimiter = Bytes.toBytes(delimiterString);
  }

  @Override
  public byte[] getRestrictedSplitPoint(byte[] splitPoint) {
    if (delimiter != null) {
      // find the first occurrence of delimiter in split point
      int index = org.apache.hbase.thirdparty.com.google.common.primitives.Bytes.indexOf(splitPoint,
        delimiter);
      if (index < 0) {
        LOG.warn("Delimiter {} not found for split key {}", Bytes.toString(delimiter),
          Bytes.toStringBinary(splitPoint));
        return splitPoint;
      }

      // group split keys by a prefix
      return Arrays.copyOf(splitPoint, Math.min(index, splitPoint.length));
    } else {
      return splitPoint;
    }
  }
}
