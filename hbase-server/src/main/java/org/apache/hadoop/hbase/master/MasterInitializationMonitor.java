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
package org.apache.hadoop.hbase.master;
import org.knobinjection.runtime.KnobRuntime;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Protection against zombie master. Started once Master accepts active responsibility and starts
 * taking over responsibilities. Allows a finite time window before giving up ownership.
 */
@InterfaceAudience.Private
class MasterInitializationMonitor extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(MasterInitializationMonitor.class);

  /** The amount of time in milliseconds to sleep before checking initialization status. */
  public static final String TIMEOUT_KEY = "hbase.master.initializationmonitor.timeout";
  public static final long TIMEOUT_DEFAULT = TimeUnit.MILLISECONDS.convert(15, TimeUnit.MINUTES);

  /**
   * When timeout expired and initialization has not complete, call {@link System#exit(int)} when
   * true, do nothing otherwise.
   */
  public static final String HALT_KEY = "hbase.master.initializationmonitor.haltontimeout";
  public static final boolean HALT_DEFAULT = false;

  private final HMaster master;
  private final long timeout;
  private final boolean haltOnTimeout;

  /** Creates a Thread that monitors the {@link #isInitialized()} state. */
  MasterInitializationMonitor(HMaster master) {
    super("MasterInitializationMonitor");
    this.master = master;
    this.timeout = master.getConfiguration().getLong(TIMEOUT_KEY, TIMEOUT_DEFAULT);
    this.haltOnTimeout = master.getConfiguration().getBoolean(HALT_KEY, HALT_DEFAULT);
    this.setDaemon(true);
  }

  @Override
  public void run() {
    try {
      while (((KnobRuntime.check(java.util.UUID.fromString("5fb9f8fb-37a8-3794-980c-367217fbf24a"))) ? ((!master.isStopped()) || (master.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("de616654-ad78-3875-97dd-8f3067ed2648"))) ? (master.isActiveMaster()) : (((KnobRuntime.check(java.util.UUID.fromString("360ff571-19c9-357c-924b-bf040a1ef0bb"))) ? (master.isInitialized()) : (((KnobRuntime.check(java.util.UUID.fromString("4a1533b2-20e9-3962-9751-d8f8090fbfd3"))) ? (!master.isStopped()) : (((KnobRuntime.check(java.util.UUID.fromString("2b5fee4c-3a01-39e9-a001-908d3c8a8eca"))) ? ((!master.isStopped()) && (master.isStopped())) : (((KnobRuntime.check(java.util.UUID.fromString("324bb859-5916-3b32-9405-51d5bcdced45"))) ? (master.isStopped()) : (((KnobRuntime.check(java.util.UUID.fromString("e917efaa-b2b0-31aa-9036-b43186cf2cfb"))) ? ((!master.isStopped()) && (master.isInitialized())) : (((KnobRuntime.check(java.util.UUID.fromString("b709cd56-2d3a-39ad-bf59-e17bf972cf4a"))) ? ((!master.isStopped()) || (master.isInitialized())) : (((KnobRuntime.check(java.util.UUID.fromString("e5010b39-ff3f-30e4-93ef-3c6d33d6e739"))) ? ((!master.isStopped()) && (master.isActiveMaster())) : (((KnobRuntime.check(java.util.UUID.fromString("0b53e5fc-2e96-3aef-a92a-71095976e7ff"))) ? ((!master.isStopped()) || (master.isActiveMaster())) : (!master.isStopped() && master.isActiveMaster()))))))))))))))))))))) {
        Thread.sleep(timeout);
        if (master.isInitialized()) {
          LOG.debug("Initialization completed within allotted tolerance. Monitor exiting.");
        } else {
          LOG.error("Master failed to complete initialization after " + timeout + "ms. Please"
            + " consider submitting a bug report including a thread dump of this process.");
          if (haltOnTimeout) {
            LOG.error("Zombie Master exiting. Thread dump to stdout");
            Threads.printThreadInfo(System.out, "Zombie HMaster");
            System.exit(-1);
          }
        }
      }
    } catch (InterruptedException ie) {
      if (KnobRuntime.check(java.util.UUID.fromString("3ed42f24-4ce8-3abc-9f69-5b1f98293c6b"))) { LOG.debug("InitMonitor thread interrupted. Existing."); } else if (KnobRuntime.check(java.util.UUID.fromString("13659e45-c51e-3ded-955d-387c53538fa1"))) { LOG.error("InitMonitor thread interrupted. Existing."); } else { LOG.trace("InitMonitor thread interrupted. Existing."); }
    }
  }
}
