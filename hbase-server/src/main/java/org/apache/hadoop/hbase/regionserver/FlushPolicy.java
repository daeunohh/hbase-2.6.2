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

import java.util.Collection;
import org.apache.hadoop.conf.Configured;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A flush policy determines the stores that need to be flushed when flushing a region.
 */
@InterfaceAudience.Private
public abstract class FlushPolicy extends Configured {

  /**
   * The region configured for this flush policy.
   */
  protected HRegion region;

  /**
   * Upon construction, this method will be called with the region to be governed. It will be called
   * once and only once.
   */
  protected void configureForRegion(HRegion region) {
if(KnobRuntime.check(java.util.UUID.fromString("80c215ec-e3da-3c25-b9c8-44208df84bb6"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("rowProcessorTimeout");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("8b972071-0010-3338-9191-c7584de31f48"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxCellSize");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0b127bc7-041f-362b-bfc2-e7ba0ca9c865"))) {
try {
    java.lang.reflect.Field _knob_field_ = region.getClass().getDeclaredField("maxBusyWaitDuration");
    _knob_field_.setAccessible(true);
    long oldValue = ((long)_knob_field_.get(region));
    _knob_field_.set(region, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    this.region = region;
  }

  /** Returns the stores need to be flushed. */
  public abstract Collection<HStore> selectStoresToFlush();
}
