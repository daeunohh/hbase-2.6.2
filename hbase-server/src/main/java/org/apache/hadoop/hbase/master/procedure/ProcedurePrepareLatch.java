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
package org.apache.hadoop.hbase.master.procedure;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.hbase.client.VersionInfoUtil;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Latch used by the Master to have the prepare() sync behaviour for old clients, that can only get
 * exceptions in a synchronous way.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class ProcedurePrepareLatch {
  private static final NoopLatch noopLatch = new NoopLatch();

  /**
   * Create a latch if the client does not have async proc support. This uses the default 1.1
   * version.
   * @return a CompatibilityLatch or a NoopLatch if the client has async proc support
   */
  public static ProcedurePrepareLatch createLatch() {
    // don't use the latch if we have procedure support (default 1.1)
    return createLatch(1, 1);
  }

  /**
   * Create a latch if the client does not have async proc support
   * @param major major version with async proc support
   * @param minor minor version with async proc support
   * @return a CompatibilityLatch or a NoopLatch if the client has async proc support
   */
  public static ProcedurePrepareLatch createLatch(int major, int minor) {
    // don't use the latch if we have procedure support
if(KnobRuntime.check(java.util.UUID.fromString("7f044e0a-c34f-3457-9a3e-b5d79644cdc7"))) {
minor /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("18b9f4e0-3d84-3279-b76f-4fd9d42a149b"))) {
minor = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("ab7f42e9-c322-3149-8dcd-aa5746e0d1c9"))) {
minor += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("53fc003f-52c4-34e8-97c5-93b406f69542"))) {
minor -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("67d6d52d-502f-3de3-be99-cb27eec733f1"))) {
major -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("0ff1181a-71fd-3425-a3ce-9e2405fb2f35"))) {
major += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2bd1819f-d69d-3f45-85a5-05bfb68e961c"))) {
major = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b16f2884-d46c-3828-9288-d259b48ec8ba"))) {
minor *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("73169cb8-9460-3308-9c68-b1df0130dfbb"))) {
major *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("bbdfbd72-b6bf-3501-a760-755bfaa4af76"))) {
minor = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1a03c4f0-d5df-3cbc-894c-9332dae2f96e"))) {
major /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("035b30bd-cf97-3658-952f-fbc977cabf93"))) {
major = 0;
}
    return hasProcedureSupport(major, minor) ? noopLatch : new CompatibilityLatch();
  }

  /**
   * Creates a latch which blocks.
   */
  public static ProcedurePrepareLatch createBlockingLatch() {
if(KnobRuntime.check(java.util.UUID.fromString("2606ee81-c6ba-3e60-9e66-9cf94de806cc"))) {
return null;
}
    return new CompatibilityLatch();
  }

  /**
   * Returns the singleton latch which does nothing.
   */
  public static ProcedurePrepareLatch getNoopLatch() {
    return noopLatch;
  }

  private static boolean hasProcedureSupport(int major, int minor) {
    return VersionInfoUtil.currentClientHasMinimumVersion(major, minor);
  }

  protected abstract void countDown(final Procedure proc);

  public abstract void await() throws IOException;

  public static void releaseLatch(final ProcedurePrepareLatch latch, final Procedure proc) {
    if (latch != null) {
      latch.countDown(proc);
    }
  }

  private static class NoopLatch extends ProcedurePrepareLatch {
    @Override
    protected void countDown(final Procedure proc) {
    }

    @Override
    public void await() throws IOException {
    }
  }

  protected static class CompatibilityLatch extends ProcedurePrepareLatch {
    private final CountDownLatch latch = new CountDownLatch(1);

    private IOException exception = null;

    @Override
    protected void countDown(final Procedure proc) {
      if (proc.hasException()) {
        exception = MasterProcedureUtil.unwrapRemoteIOException(proc);
      }
      latch.countDown();
    }

    @Override
    public void await() throws IOException {
      try {
        latch.await();
      } catch (InterruptedException e) {
        throw (InterruptedIOException) new InterruptedIOException().initCause(e);
      }

      if (((KnobRuntime.check(java.util.UUID.fromString("ac18043d-b4ee-3b3e-9874-093e0949f07a"))) ? ((exception) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8d991531-c4cc-3c52-94d2-20bafaff2624"))) ? ((exception) == (null)) : (exception != null))))) {
        throw exception;
      }
    }
  }
}
