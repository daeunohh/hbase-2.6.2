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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A split restriction that restricts the pattern of the split point.
 * <p>
 * The difference between {@link RegionSplitPolicy} and RegionSplitRestriction is that
 * RegionSplitRestriction defines how to split while {@link RegionSplitPolicy} defines when we need
 * to split.
 * <p>
 * We can specify a split restriction, "KeyPrefix" or "DelimitedKeyPrefix", to a table with the
 * "hbase.regionserver.region.split_restriction.type" property. The "KeyPrefix" split restriction
 * groups rows by a prefix of the row-key. And the "DelimitedKeyPrefix" split restriction groups
 * rows by a prefix of the row-key with a delimiter. For example:
 *
 * <pre>
 * <code>
 * # Create a table with a "KeyPrefix" split restriction, where the prefix length is 2 bytes
 * hbase> create 'tbl1', 'fam',
 *   {CONFIGURATION => {'hbase.regionserver.region.split_restriction.type' => 'KeyPrefix',
 *                      'hbase.regionserver.region.split_restriction.prefix_length' => '2'}}
 *
 * # Create a table with a "DelimitedKeyPrefix" split restriction, where the delimiter is a comma
 * hbase> create 'tbl2', 'fam',
 *   {CONFIGURATION => {'hbase.regionserver.region.split_restriction.type' => 'DelimitedKeyPrefix',
 *                      'hbase.regionserver.region.split_restriction.delimiter' => ','}}
 * </code>
 * </pre>
 *
 * Instead of specifying a split restriction to a table directly, we can also set the properties in
 * hbase-site.xml. In this case, the specified split restriction is applied for all the tables.
 * <p>
 * Note that the split restriction is also applied to a user-specified split point so that we don't
 * allow users to break the restriction.
 * @see NoRegionSplitRestriction
 * @see KeyPrefixRegionSplitRestriction
 * @see DelimitedKeyPrefixRegionSplitRestriction
 */
@InterfaceAudience.Private
public abstract class RegionSplitRestriction {
  private static final Logger LOG = LoggerFactory.getLogger(RegionSplitRestriction.class);

  public static final String RESTRICTION_TYPE_KEY =
    "hbase.regionserver.region.split_restriction.type";

  public static final String RESTRICTION_TYPE_NONE = "None";
  public static final String RESTRICTION_TYPE_KEY_PREFIX = "KeyPrefix";
  public static final String RESTRICTION_TYPE_DELIMITED_KEY_PREFIX = "DelimitedKeyPrefix";

  /**
   * Create the RegionSplitRestriction configured for the given table.
   * @param tableDescriptor the table descriptor
   * @param conf            the configuration
   * @return a RegionSplitRestriction instance
   * @throws IOException if an error occurs
   */
  public static RegionSplitRestriction create(TableDescriptor tableDescriptor, Configuration conf)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("9937e975-b21c-30ae-87d4-e3ff7bd6ee44"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("74f5f5d1-bbf7-33d1-94df-a0f49bfce32c"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("cdec4783-25da-3db1-96a4-861041cad033"))) {
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
    String type = tableDescriptor.getValue(RESTRICTION_TYPE_KEY);
    if (((KnobRuntime.check(java.util.UUID.fromString("e62e4c2c-908c-3039-8fe3-94637dbcb2d7"))) ? ((type) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("c080d080-253f-3a00-9bc3-daef0ff4b37b"))) ? ((type) == (null)) : (type == null))))) {
      type = conf.get(RESTRICTION_TYPE_KEY, RESTRICTION_TYPE_NONE);
    }

    RegionSplitRestriction ret;
    switch (type) {
      case RESTRICTION_TYPE_NONE:
        ret = new NoRegionSplitRestriction();
        break;
      case RESTRICTION_TYPE_KEY_PREFIX:
        ret = new KeyPrefixRegionSplitRestriction();
        break;
      case RESTRICTION_TYPE_DELIMITED_KEY_PREFIX:
        ret = new DelimitedKeyPrefixRegionSplitRestriction();
        break;
      default:
        LOG.warn("Invalid RegionSplitRestriction type specified: {}. "
          + "Using the default RegionSplitRestriction", type);
        ret = new NoRegionSplitRestriction();
        break;
    }
if(KnobRuntime.check(java.util.UUID.fromString("a8065ca3-af7a-36d9-8a6e-2c048d8c2d14"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("6851d211-7c90-36c2-a116-8065ff5a2056"))) {
throw new java.io.IOException("Injected exception");
}
    ret.initialize(tableDescriptor, conf);
    return ret;
  }

  /**
   * Initialize the RegionSplitRestriction instance
   * @param tableDescriptor the table descriptor
   * @param conf            the configuration
   * @throws IOException if an error occurs
   */
  public abstract void initialize(TableDescriptor tableDescriptor, Configuration conf)
    throws IOException;

  /**
   * Returns a restricted split point.
   * @param splitPoint the split point determined by {@link RegionSplitPolicy} or specified by a
   *                   user manually
   * @return the restricted split point
   */
  public abstract byte[] getRestrictedSplitPoint(byte[] splitPoint);
}
