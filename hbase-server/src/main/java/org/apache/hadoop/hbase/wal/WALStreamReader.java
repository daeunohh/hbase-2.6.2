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
package org.apache.hadoop.hbase.wal;
import org.knobinjection.runtime.KnobRuntime;

import java.io.Closeable;
import java.io.IOException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A one way WAL reader, without reset and seek support.
 * <p/>
 * In most cases you should use this interface to read WAL file, as the implementation is simple and
 * robust. For replication, where we want to tail the WAL file which is currently being written, you
 * should use {@link WALTailingReader} instead.
 * @see WALTailingReader
 */
@InterfaceAudience.Private
public interface WALStreamReader extends Closeable {

  /**
   * Read the next entry in WAL.
   * <p/>
   * In most cases you should just use this method, especially when reading a closed wal file for
   * splitting or printing.
   */
  default WAL.Entry next() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("69645510-017a-372f-a32e-39e2731da64c"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("ab8226a3-1bf3-3d77-9f14-30cd304483a7"))) {
return null;
}
    return next(null);
  }

  /**
   * Read the next entry in WAL, use the given {@link WAL.Entry} if not {@code null} to hold the
   * data.
   * <p/>
   * Mainly used in MR.
   * @param reuse the entry to be used for reading, can be {@code null}
   */
  WAL.Entry next(WAL.Entry reuse) throws IOException;

  /**
   * Get the current reading position.
   */
  long getPosition() throws IOException;

  /**
   * Override to remove the 'throws IOException' as we are just a reader.
   */
  @Override
  void close();
}
