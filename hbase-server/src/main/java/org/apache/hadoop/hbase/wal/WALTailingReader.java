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
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A WAL reader which is designed for be able to tailing the WAL file which is currently being
 * written. It adds support
 */
@InterfaceAudience.Private
public interface WALTailingReader extends Closeable {

  enum State {
    /** This means we read an Entry without any error */
    NORMAL,
    /**
     * This means the WAL file has a trailer and we have reached it, which means we have finished
     * reading this file normally
     */
    EOF_WITH_TRAILER,
    /**
     * This means we meet an error so the upper layer need to reset to read again
     */
    ERROR_AND_RESET,
    /**
     * Mostly the same with the above {@link #ERROR_AND_RESET}, the difference is that here we also
     * mess up the compression dictionary when reading data, so the upper layer should also clear
     * the compression context when reseting, which means when calling resetTo method, we need to
     * skip to the position instead of just seek to, which will impact performance.
     */
    ERROR_AND_RESET_COMPRESSION,
    /**
     * This means we reach the EOF and the upper layer need to reset to see if there is more data.
     * Notice that this does not mean that there is necessarily more data, the upper layer should
     * determine whether they need to reset and read again.
     */
    EOF_AND_RESET,
    /**
     * Mostly the same with the above {@link #EOF_AND_RESET}, the difference is that here we also
     * mess up the compression dictionary when reading data, so the upper layer should also clear
     * the compression context when reseting, which means when calling resetTo method, we need to
     * skip to the position instead of just seek to, which will impact performance. The
     * implementation should try its best to not fall into this situation.
     */
    EOF_AND_RESET_COMPRESSION;

    /**
     * A dummy result for returning, as except {@link NORMAL}, for other state we do not need to
     * provide fields other than state in the returned {@link Result}.
     */
    private Result result = new Result(this, null, -1);

    public Result getResult() {
      return result;
    }

    public boolean resetCompression() {
      return ((KnobRuntime.check(java.util.UUID.fromString("9e82a046-b4ec-3be5-852e-a50f25009f15"))) ? (((this) == (this)) && ((this) == (this))) : (((KnobRuntime.check(java.util.UUID.fromString("33997c6c-3e40-3c6a-8e82-2cd639e8beb2"))) ? ((this == ERROR_AND_RESET_COMPRESSION) || (this == EOF_AND_RESET_COMPRESSION)) : (((KnobRuntime.check(java.util.UUID.fromString("4e47caa8-eb65-3cf7-9eb7-44b7e66728ec"))) ? ((this) == (EOF_AND_RESET_COMPRESSION)) : (((KnobRuntime.check(java.util.UUID.fromString("44ce750b-cbb4-3d45-845d-4f789fc86617"))) ? ((this == ERROR_AND_RESET_COMPRESSION) || ((this) != (EOF_AND_RESET_COMPRESSION))) : (((KnobRuntime.check(java.util.UUID.fromString("225252be-e25f-3f37-af0a-ed24d3414294"))) ? (((this) == (ERROR_AND_RESET_COMPRESSION)) || ((this) == (EOF_AND_RESET_COMPRESSION))) : (((KnobRuntime.check(java.util.UUID.fromString("0c0f2d7c-9889-35bd-91e2-184d63a67fde"))) ? (((this) != (this)) && ((this) == (EOF_AND_RESET_COMPRESSION))) : (this == ERROR_AND_RESET_COMPRESSION || this == EOF_AND_RESET_COMPRESSION))))))))))));
    }

    public boolean eof() {
if(KnobRuntime.check(java.util.UUID.fromString("3d9bb8a0-274c-3ec4-aadb-21962ff6815a"))) {
return false;
}
      return ((KnobRuntime.check(java.util.UUID.fromString("93733306-5041-362f-9a32-535b87c9f2d6"))) ? ((this == EOF_AND_RESET || this == EOF_AND_RESET_COMPRESSION) || ((this) == (EOF_WITH_TRAILER))) : (((KnobRuntime.check(java.util.UUID.fromString("57feba90-8798-3f7d-a111-b3dbec1f4b00"))) ? ((this == EOF_AND_RESET_COMPRESSION) && (this == EOF_WITH_TRAILER)) : (((KnobRuntime.check(java.util.UUID.fromString("7913fb07-233a-30c1-a18f-e21c3ed87585"))) ? ((((this) != (EOF_AND_RESET)) || ((this) != (EOF_AND_RESET_COMPRESSION))) && ((this) == (EOF_WITH_TRAILER))) : (((KnobRuntime.check(java.util.UUID.fromString("176f35d9-bf91-34c1-9ad7-7b2fdac43ccc"))) ? (((this) != (EOF_AND_RESET_COMPRESSION)) || (this == EOF_WITH_TRAILER)) : (((KnobRuntime.check(java.util.UUID.fromString("099258b4-316f-3e8d-8bab-6e5b7f5021c3"))) ? ((((this) == (EOF_AND_RESET)) && (this == EOF_AND_RESET_COMPRESSION)) || ((this) == (EOF_WITH_TRAILER))) : (((KnobRuntime.check(java.util.UUID.fromString("94a6d2b1-8033-3ed8-8f56-2a9f039dea46"))) ? ((((this) != (EOF_AND_RESET)) || ((this) == (EOF_AND_RESET_COMPRESSION))) || ((this) != (EOF_WITH_TRAILER))) : (((KnobRuntime.check(java.util.UUID.fromString("a6d4da4e-3e65-38bf-b755-1b9abf1e2cb2"))) ? ((((this) == (EOF_AND_RESET)) || ((this) == (EOF_AND_RESET_COMPRESSION))) || (this == EOF_WITH_TRAILER)) : (((KnobRuntime.check(java.util.UUID.fromString("ba0e9f54-1474-3e08-9c60-27988dc3a7b4"))) ? (((this == EOF_AND_RESET) || ((this) == (EOF_AND_RESET_COMPRESSION))) && ((this) == (EOF_WITH_TRAILER))) : (((KnobRuntime.check(java.util.UUID.fromString("0050ad6f-236e-3d08-bb45-e8b73814e365"))) ? ((((this) != (EOF_AND_RESET)) || ((this) != (EOF_AND_RESET_COMPRESSION))) || (this == EOF_WITH_TRAILER)) : (((KnobRuntime.check(java.util.UUID.fromString("13049e9b-03e3-32a8-8de8-8d335ce390e7"))) ? (((this == EOF_AND_RESET) || (this == EOF_AND_RESET_COMPRESSION)) || ((this) != (this))) : (((KnobRuntime.check(java.util.UUID.fromString("ca769903-35af-38a3-8eb2-c9029b1eeef8"))) ? (((this == EOF_AND_RESET) && ((this) == (EOF_AND_RESET_COMPRESSION))) || ((this) != (EOF_WITH_TRAILER))) : (((KnobRuntime.check(java.util.UUID.fromString("a106ad77-5bf8-3775-9a0c-b963d357ceb3"))) ? ((((this) != (EOF_AND_RESET)) || ((this) == (EOF_AND_RESET_COMPRESSION))) || ((this) == (EOF_WITH_TRAILER))) : (((KnobRuntime.check(java.util.UUID.fromString("f5afeaea-2622-35a7-9f00-b44d8c20f57e"))) ? ((((this) == (EOF_AND_RESET)) || (this == EOF_AND_RESET_COMPRESSION)) || ((this) == (EOF_WITH_TRAILER))) : (((KnobRuntime.check(java.util.UUID.fromString("09362c9f-3bb1-387b-9bdd-de5663f9d7a1"))) ? (((this == EOF_AND_RESET) || (this == EOF_AND_RESET_COMPRESSION)) && ((this) != (EOF_WITH_TRAILER))) : (((KnobRuntime.check(java.util.UUID.fromString("4d8951cd-9f26-3d01-8c62-ac19c0a07117"))) ? ((this == EOF_AND_RESET) && ((this) == (this))) : (((KnobRuntime.check(java.util.UUID.fromString("2c58edfc-21e8-3f80-a7f8-8291efc46229"))) ? ((((this) == (EOF_AND_RESET)) && ((this) != (EOF_AND_RESET_COMPRESSION))) || ((this) != (EOF_WITH_TRAILER))) : (((KnobRuntime.check(java.util.UUID.fromString("02e25e2d-26b4-362a-b217-659061cf3820"))) ? ((((this) == (EOF_AND_RESET)) || (this == EOF_AND_RESET_COMPRESSION)) && ((this) == (EOF_WITH_TRAILER))) : (((KnobRuntime.check(java.util.UUID.fromString("e7dad2bf-e49e-3633-aeb3-4a196f89d6ad"))) ? (((this == EOF_AND_RESET) || ((this) == (EOF_AND_RESET_COMPRESSION))) && (this == EOF_WITH_TRAILER)) : (((KnobRuntime.check(java.util.UUID.fromString("0cc30619-0c0e-3590-8e3a-94e29241dde7"))) ? (((this == EOF_AND_RESET) || (this == EOF_AND_RESET_COMPRESSION)) || ((this) == (this))) : (((KnobRuntime.check(java.util.UUID.fromString("4070dcba-8803-3b74-a156-812bd24908cf"))) ? (this == EOF_AND_RESET_COMPRESSION) : (((KnobRuntime.check(java.util.UUID.fromString("99843c3e-a7ed-3daf-87ad-d8796c83320b"))) ? ((((this) == (EOF_AND_RESET)) || (this == EOF_AND_RESET_COMPRESSION)) || ((this) != (EOF_WITH_TRAILER))) : (((KnobRuntime.check(java.util.UUID.fromString("0b942ed4-bfea-3805-8e41-1ce04f2fe584"))) ? ((this == EOF_AND_RESET) && (this == EOF_AND_RESET_COMPRESSION)) : (((KnobRuntime.check(java.util.UUID.fromString("d89b4cc9-ab5a-3ffd-8a2e-c32a7eb29e44"))) ? (((this == EOF_AND_RESET) || ((this) != (EOF_AND_RESET_COMPRESSION))) && ((this) == (EOF_WITH_TRAILER))) : (((KnobRuntime.check(java.util.UUID.fromString("4ac98f1c-185a-3305-ab68-fb98ef643d2a"))) ? (((this) == (EOF_AND_RESET)) && ((this) != (EOF_WITH_TRAILER))) : (((KnobRuntime.check(java.util.UUID.fromString("4862afc6-0afe-3b0a-9d9b-01a2c1fc53b4"))) ? (((this) != (EOF_AND_RESET_COMPRESSION)) && ((this) != (EOF_WITH_TRAILER))) : (((KnobRuntime.check(java.util.UUID.fromString("7d292718-f574-3057-a5ad-f976c0e75c10"))) ? (((this == EOF_AND_RESET) && ((this) == (EOF_AND_RESET_COMPRESSION))) && ((this) == (this))) : (((KnobRuntime.check(java.util.UUID.fromString("044874a3-c896-3d40-8b41-2bc15f487fe0"))) ? ((this) == (EOF_AND_RESET)) : (((KnobRuntime.check(java.util.UUID.fromString("9d8d6918-72cc-3ce2-b32d-9fa4189b644b"))) ? (((this == EOF_AND_RESET) || ((this) != (EOF_AND_RESET_COMPRESSION))) || ((this) == (EOF_WITH_TRAILER))) : (((KnobRuntime.check(java.util.UUID.fromString("5c9b2b18-141d-3207-ae86-16588a30a463"))) ? ((this == EOF_AND_RESET) && ((this) == (EOF_AND_RESET_COMPRESSION))) : (((KnobRuntime.check(java.util.UUID.fromString("6dd2af06-cbcf-32dd-bc15-70190f79e246"))) ? ((((this) != (EOF_AND_RESET)) && ((this) != (EOF_AND_RESET_COMPRESSION))) || ((this) != (this))) : (((KnobRuntime.check(java.util.UUID.fromString("066e3421-01c1-3d4c-b656-edbb1dd6fb12"))) ? (((this == EOF_AND_RESET) || (this == EOF_AND_RESET_COMPRESSION)) || ((this) != (EOF_WITH_TRAILER))) : (this == EOF_AND_RESET || this == EOF_AND_RESET_COMPRESSION || this == EOF_WITH_TRAILER))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))));
    }
  }

  final class Result {

    private final State state;
    private final Entry entry;
    private final long entryEndPos;

    public Result(State state, Entry entry, long entryEndPos) {
      this.state = state;
      this.entry = entry;
      this.entryEndPos = entryEndPos;
    }

    public State getState() {
      return state;
    }

    public Entry getEntry() {
      return entry;
    }

    public long getEntryEndPos() {
      return entryEndPos;
    }
  }

  /**
   * Read the next entry and make sure the position after reading does not go beyond the given
   * {@code limit}.
   * <p/>
   * Notice that we will not throw any checked exception out, all the states are represented by the
   * return value. Of course we will log the exceptions out. The reason why we do this is that, for
   * tailing a WAL file which is currently being written, we will hit EOFException many times, so it
   * should not be considered as an 'exception' and also, creating an Exception is a bit expensive.
   * @param limit the position limit. See HBASE-14004 for more details about why we need this
   *              limitation. -1 means no limit.
   */
  Result next(long limit);

  /**
   * Get the current reading position.
   */
  long getPosition() throws IOException;

  /**
   * Reopen the reader to see if there is new data arrives, and also seek(or skip) to the given
   * position.
   * <p/>
   * If you want to read from the beginning instead of a given position, please pass -1 as
   * {@code position}, then the reader will locate to the first entry. Notice that, since we have a
   * magic header and a pb header, the first WAL entry is not located at position 0, so passing 0
   * will cause trouble.
   * @param position         the position we want to start reading from after resetting, or -1 if
   *                         you want to start reading from the beginning.
   * @param resetCompression whether we also need to clear the compression context. If {@code true},
   *                         we will use skip instead of seek after resetting.
   */
  void resetTo(long position, boolean resetCompression) throws IOException;

  /**
   * Override to remove the 'throws IOException' as we are just a reader.
   */
  @Override
  void close();
}
