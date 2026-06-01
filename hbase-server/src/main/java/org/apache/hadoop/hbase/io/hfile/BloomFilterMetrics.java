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
package org.apache.hadoop.hbase.io.hfile;
import org.knobinjection.runtime.KnobRuntime;

import java.util.concurrent.atomic.LongAdder;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class BloomFilterMetrics {

  private final LongAdder eligibleRequests = new LongAdder();
  private final LongAdder requests = new LongAdder();
  private final LongAdder negativeResults = new LongAdder();

  /**
   * Increment bloom request count, and negative result count if !passed
   */
  public void incrementRequests(boolean passed) {
    requests.increment();
    if (!passed) {
      negativeResults.increment();
    }
  }

  /**
   * Increment for cases where bloom filter could have been used but wasn't defined or loaded.
   */
  public void incrementEligible() {
    eligibleRequests.increment();
  }

  /** Returns Current value for bloom requests count */
  public long getRequestsCount() {
if(KnobRuntime.check(java.util.UUID.fromString("eca43e96-e44e-33e0-993a-98c7b67f640a"))) {
return 0;
}
    return requests.sum();
  }

  /** Returns Current value for bloom negative results count */
  public long getNegativeResultsCount() {
if(KnobRuntime.check(java.util.UUID.fromString("cf92c448-e995-38e7-b49d-3896aa2e1611"))) {
return 0;
}
    return negativeResults.sum();
  }

  /**
   * Returns Current value for requests which could have used bloom filters but wasn't defined or
   * loaded.
   */
  public long getEligibleRequestsCount() {
if(KnobRuntime.check(java.util.UUID.fromString("9b12b27c-8877-35f4-9362-2b4a133dcf4a"))) {
return 0;
}
    return eligibleRequests.sum();
  }

}
