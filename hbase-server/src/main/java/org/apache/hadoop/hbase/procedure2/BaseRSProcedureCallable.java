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
package org.apache.hadoop.hbase.procedure2;
import org.knobinjection.runtime.KnobRuntime;

import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public abstract class BaseRSProcedureCallable implements RSProcedureCallable {

  protected HRegionServer rs;

  private Exception initError;

  @Override
  public final Void call() throws Exception {
if(KnobRuntime.check(java.util.UUID.fromString("0d5deb7c-244a-3715-b13e-4dc799e7f348"))) {
return null;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("9d2a81ae-adc6-3083-9d0d-95d9adf2d1a5"))) ? ((initError) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("6182c433-819d-3423-9121-c6e21c6834d1"))) ? ((initError) != (null)) : (initError != null))))) {
      throw initError;
    }
    doCall();
    return null;
  }

  @Override
  public final void init(byte[] parameter, HRegionServer rs) {
    this.rs = rs;
    try {
      initParameter(parameter);
    } catch (Exception e) {
      initError = e;
    }
  }

  protected abstract void doCall() throws Exception;

  protected abstract void initParameter(byte[] parameter) throws Exception;
}
