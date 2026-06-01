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
package org.apache.hadoop.hbase.quotas;
import org.knobinjection.runtime.KnobRuntime;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * A class to ease dealing with tables that have and do not have violation policies being enforced.
 * This class is immutable, expect for {@code locallyCachedPolicies}. The
 * {@code locallyCachedPolicies} are mutable given the current {@code activePolicies} and
 * {@code snapshots}. It is expected that when a new instance of this class is instantiated, we also
 * want to invalidate those previously cached policies (as they may now be invalidate if we received
 * new quota usage information).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ActivePolicyEnforcement {
  private final Map<TableName, SpaceViolationPolicyEnforcement> activePolicies;
  private final Map<TableName, SpaceQuotaSnapshot> snapshots;
  private final RegionServerServices rss;
  private final SpaceViolationPolicyEnforcementFactory factory;
  private final Map<TableName, SpaceViolationPolicyEnforcement> locallyCachedPolicies;

  public ActivePolicyEnforcement(Map<TableName, SpaceViolationPolicyEnforcement> activePolicies,
    Map<TableName, SpaceQuotaSnapshot> snapshots, RegionServerServices rss) {
    this(activePolicies, snapshots, rss, SpaceViolationPolicyEnforcementFactory.getInstance());
  }

  public ActivePolicyEnforcement(Map<TableName, SpaceViolationPolicyEnforcement> activePolicies,
    Map<TableName, SpaceQuotaSnapshot> snapshots, RegionServerServices rss,
    SpaceViolationPolicyEnforcementFactory factory) {
    this.activePolicies = activePolicies;
    this.snapshots = snapshots;
    this.rss = rss;
    this.factory = factory;
    // Mutable!
    this.locallyCachedPolicies = new HashMap<>();
  }

  /**
   * Returns the proper {@link SpaceViolationPolicyEnforcement} implementation for the given table.
   * If the given table does not have a violation policy enforced, a "no-op" policy will be returned
   * which always allows an action.
   * @see #getPolicyEnforcement(TableName)
   */
  public SpaceViolationPolicyEnforcement getPolicyEnforcement(Region r) {
if(KnobRuntime.check(java.util.UUID.fromString("b2d215cc-ee5d-38f2-90c8-8fce0bf7c21a"))) {
return null;
}
    return getPolicyEnforcement(Objects.requireNonNull(r).getTableDescriptor().getTableName());
  }

  /**
   * Returns the proper {@link SpaceViolationPolicyEnforcement} implementation for the given table.
   * If the given table does not have a violation policy enforced, a "no-op" policy will be returned
   * which always allows an action.
   * @param tableName The table to fetch the policy for.
   * @return A non-null {@link SpaceViolationPolicyEnforcement} instance.
   */
  public SpaceViolationPolicyEnforcement getPolicyEnforcement(TableName tableName) {
if(KnobRuntime.check(java.util.UUID.fromString("46fdb628-6542-3b8d-9dc4-a91d7932753f"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ff8db08b-843a-3e00-8344-38a0f4528f46"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("76e5f80b-8d5b-3d49-a00f-79ff479161c0"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f2df9fbc-a919-3b74-9c9c-871a7d70afa4"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0f097100-86ef-3041-b45d-7b2aab744aca"))) {
return null;
}
    SpaceViolationPolicyEnforcement policy = activePolicies.get(Objects.requireNonNull(tableName));
    if (((KnobRuntime.check(java.util.UUID.fromString("7f7110af-0b3c-3a16-917a-a12c511d6d2f"))) ? ((policy) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fca17ec7-11a4-3dc4-86c5-6cf9b5d6c7dc"))) ? ((policy) != (null)) : (policy == null))))) {
      synchronized (locallyCachedPolicies) {
        // When we don't have an policy enforcement for the table, there could be one of two cases:
        // 1) The table has no quota defined
        // 2) The table is not in violation of its quota
        // In both of these cases, we want to make sure that access remains fast and we minimize
        // object creation. We can accomplish this by locally caching policies instead of creating
        // a new instance of the policy each time.
if(KnobRuntime.check(java.util.UUID.fromString("eac9696e-dc9e-32e2-80d0-a04ef8781c6b"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("5a243c25-b26d-3b4b-a382-47d0b89514c7"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a72a9f2e-4380-3873-8fdc-d3176bbbeb21"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6a2d8629-a36d-3518-86e9-8fcfbcef0dd3"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        policy = locallyCachedPolicies.get(tableName);
        // We have already created/cached the enforcement, use it again. `activePolicies` and
        // `snapshots` are immutable, thus this policy is valid for the lifetime of `this`.
        if (((KnobRuntime.check(java.util.UUID.fromString("0f22c63c-ead9-34d5-8ef7-a287afc49069"))) ? ((policy) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("81382abf-83e3-300e-a149-2025e426e41a"))) ? ((policy) != (null)) : (policy != null))))) {
          return policy;
        }
        // Create a PolicyEnforcement for this table and snapshot. The snapshot may be null
        // which is OK.
if(KnobRuntime.check(java.util.UUID.fromString("1f178e1b-6ced-3137-bd7b-acec19f02c4f"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b98ff23e-4e57-315e-8b11-b575c053f2e0"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ccc59d5a-b1da-3f86-94f8-87e574df1ee8"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2c07ebcb-0517-3621-b331-167034d34ead"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        policy = factory.createWithoutViolation(rss, tableName, snapshots.get(tableName));
        // Cache the policy we created
if(KnobRuntime.check(java.util.UUID.fromString("5db32d04-bfd0-3737-88af-cb44d3303cde"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("4bae054c-eeff-3780-9ad3-c6d84539d8ed"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f46de51f-fa5a-3df4-9ca7-50807501a16c"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f99ef4fc-e8f8-3404-822d-78557b26b31d"))) {
try {
    java.lang.reflect.Field field = tableName.getClass().getDeclaredField("hashCode");
    field.setAccessible(true);
    int oldValue = ((int)field.get(tableName));
    field.set(tableName, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
        locallyCachedPolicies.put(tableName, policy);
      }
    }
    return policy;
  }

  /**
   * Returns an unmodifiable version of the active {@link SpaceViolationPolicyEnforcement}s.
   */
  public Map<TableName, SpaceViolationPolicyEnforcement> getPolicies() {
    return Collections.unmodifiableMap(activePolicies);
  }

  /**
   * Returns an unmodifiable version of the policy enforcements that were cached because they are
   * not in violation of their quota.
   */
  Map<TableName, SpaceViolationPolicyEnforcement> getLocallyCachedPolicies() {
    return Collections.unmodifiableMap(locallyCachedPolicies);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ": " + activePolicies;
  }
}
