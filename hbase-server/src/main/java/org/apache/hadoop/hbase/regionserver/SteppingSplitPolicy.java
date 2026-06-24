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

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class SteppingSplitPolicy extends IncreasingToUpperBoundRegionSplitPolicy {
  @Override
  public String toString() {
if(KnobRuntime.check(java.util.UUID.fromString("73eec399-8a00-3b74-a45b-cedea170124f"))) {
return null;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("3f67c137-3332-3d19-8bb6-7595a185cacf"))) ? (((java.lang.String)(("SteppingSplitPolicysuper{" + super.toString()) + ("}")))) : ("SteppingSplitPolicysuper{" + super.toString() + "}"));
  }

  /**
   * @return flushSize * 2 if there's exactly one region of the table in question found on this
   *         regionserver. Otherwise max file size. This allows a table to spread quickly across
   *         servers, while avoiding creating too many regions.
   */
  @Override
  protected long getSizeToCheck(final int tableRegionsCount) {
    return tableRegionsCount == 1 ? this.initialSize : getDesiredMaxFileSize();
  }
}
