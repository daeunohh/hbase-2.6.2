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
package org.apache.hadoop.hbase.io;
import org.knobinjection.runtime.KnobRuntime;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The FileLink is a sort of hardlink, that allows access to a file given a set of locations.
 * <p>
 * <b>The Problem:</b>
 * <ul>
 * <li>HDFS doesn't have support for hardlinks, and this make impossible to referencing the same
 * data blocks using different names.</li>
 * <li>HBase store files in one location (e.g. table/region/family/) and when the file is not needed
 * anymore (e.g. compaction, region deletion, ...) moves it to an archive directory.</li>
 * </ul>
 * If we want to create a reference to a file, we need to remember that it can be in its original
 * location or in the archive folder. The FileLink class tries to abstract this concept and given a
 * set of locations it is able to switch between them making this operation transparent for the
 * user. {@link HFileLink} is a more concrete implementation of the {@code FileLink}.
 * <p>
 * <b>Back-references:</b> To help the {@link org.apache.hadoop.hbase.master.cleaner.CleanerChore}
 * to keep track of the links to a particular file, during the {@code FileLink} creation, a new file
 * is placed inside a back-reference directory. There's one back-reference directory for each file
 * that has links, and in the directory there's one file per link.
 * <p>
 * HFileLink Example
 * <ul>
 * <li>/hbase/table/region-x/cf/file-k (Original File)</li>
 * <li>/hbase/table-cloned/region-y/cf/file-k.region-x.table (HFileLink to the original file)</li>
 * <li>/hbase/table-2nd-cloned/region-z/cf/file-k.region-x.table (HFileLink to the original file)
 * </li>
 * <li>/hbase/.archive/table/region-x/.links-file-k/region-y.table-cloned (Back-reference to the
 * link in table-cloned)</li>
 * <li>/hbase/.archive/table/region-x/.links-file-k/region-z.table-2nd-cloned (Back-reference to the
 * link in table-2nd-cloned)</li>
 * </ul>
 */
@InterfaceAudience.Private
public class FileLink {
  private static final Logger LOG = LoggerFactory.getLogger(FileLink.class);

  /** Define the Back-reference directory name prefix: .links-&lt;hfile&gt;/ */
  public static final String BACK_REFERENCES_DIRECTORY_PREFIX = ".links-";

  /**
   * FileLink InputStream that handles the switch between the original path and the alternative
   * locations, when the file is moved.
   */
  protected static class FileLinkInputStream extends InputStream
    implements Seekable, PositionedReadable, CanSetDropBehind, CanSetReadahead, CanUnbuffer {
    private FSDataInputStream in = null;
    private Path currentPath = null;
    private long pos = 0;

    private final FileLink fileLink;
    private final int bufferSize;
    private final FileSystem fs;

    public FileLinkInputStream(final FileSystem fs, final FileLink fileLink) throws IOException {
      this(fs, fileLink, CommonFSUtils.getDefaultBufferSize(fs));
    }

    public FileLinkInputStream(final FileSystem fs, final FileLink fileLink, int bufferSize)
      throws IOException {
      this.bufferSize = bufferSize;
      this.fileLink = fileLink;
      this.fs = fs;

      this.in = tryOpen();
    }

    private FSDataInputStream getUnderlyingInputStream() {
      return in;
    }

    @Override
    public int read() throws IOException {
      int res;
      try {
        res = in.read();
      } catch (FileNotFoundException e) {
        res = tryOpen().read();
      }
      if (res > 0) {
        pos += 1;
      }
      return res;
    }

    @Override
    public int read(byte[] b) throws IOException {
      return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int n;
      try {
if(KnobRuntime.check(java.util.UUID.fromString("ab92d455-6881-3bdf-a41e-7c48a5210d88"))) {
off = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("25467a33-7102-3f2c-94e3-9ab7a0befb8a"))) {
len = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("9d219e41-7e82-3203-a38a-2df07263ba0b"))) {
len /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("cc20d218-d9e0-372c-b1ca-068bb75b6233"))) {
off = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("fda582b6-bd34-3bec-8f08-13c2c22b24e4"))) {
off *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("accb5570-4d61-3f62-b015-77f4a0cdda6f"))) {
len += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d2c17796-6f51-3237-8b52-23e43e03f568"))) {
len = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("fa0bc82f-d289-3b89-902c-ce1012912d7a"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("ea771ecb-e5db-3e6a-afad-c8eab06eeaf9"))) {
off /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("4e3f33a1-6bf0-37c4-a25f-aa30aba7307a"))) {
off -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9066ce2b-d909-329e-be33-fb9876028e8b"))) {
off += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("50342f7a-5a09-357a-bad1-b1b71c1b9827"))) {
len *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("4c8b3143-38b0-3a71-8148-3bbb345e4af4"))) {
len -= 1;
}
        n = in.read(b, off, len);
      } catch (FileNotFoundException e) {
        n = tryOpen().read(b, off, len);
      }
      if (((KnobRuntime.check(java.util.UUID.fromString("e384e7c9-0327-3101-9f4b-68b7d46a30c8"))) ? ((n) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e33829ef-3e10-33fb-8363-5372825b102f"))) ? ((n) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8b74065f-740f-34a6-9e09-ea49b6caa69c"))) ? ((n) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1893662d-df29-3640-9cab-168ba12bacfc"))) ? ((n) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("497ff01f-3fe6-30ac-853e-4a2202b3ac45"))) ? ((n) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("cc4e85aa-7d76-36b1-9efe-8553ce283ac7"))) ? ((n) == (0)) : (n > 0))))))))))))) {
        pos += n;
      }
if(KnobRuntime.check(java.util.UUID.fromString("a21b064a-f05e-3096-9647-e49385a1dfec"))) {
throw new java.io.IOException("Injected exception");
}
      assert (in.getPos() == pos);
      return n;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      int n;
      try {
if(KnobRuntime.check(java.util.UUID.fromString("76d28d3b-5580-320b-82ef-ae30e210c448"))) {
length = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("f6996b60-8fbf-339d-b8b8-5d12c4bb0f00"))) {
length += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("af2beafb-be5c-3529-8ff1-c8ce79fbeea1"))) {
offset /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("4c47ee5d-5f9e-3889-b8ad-4059c97327de"))) {
length -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("210b9518-e859-362e-92f5-e16e6d392395"))) {
position += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("db8001de-fc76-32f1-92a8-b153541342df"))) {
position = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("42576047-88ba-39e0-9f3c-f00f76c22119"))) {
offset *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("fedffab8-b6a9-32e2-925c-abde418353c9"))) {
length *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("c9da4658-90eb-3a2a-bdb9-0decf5fa7ec5"))) {
position = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("d4e9a8da-36a2-38a1-9d6c-4eed765deffc"))) {
offset = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("08a847df-8b47-3608-bb61-305d4ebd0591"))) {
length = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1ff85949-8309-3579-8396-55ad12a53a4d"))) {
offset -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("573b4c19-3742-3303-81fc-7dedcdde7bd6"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("2195e40b-ad4a-3b07-829b-1f2e05a72e06"))) {
length /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("0db46e38-806c-336b-bdcd-3c4a456146c5"))) {
offset += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("44c0266f-8c37-3f22-9776-e94ef07b14fb"))) {
offset = -1;
}
        n = in.read(position, buffer, offset, length);
      } catch (FileNotFoundException e) {
        n = tryOpen().read(position, buffer, offset, length);
      }
      return n;
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      try {
        in.readFully(position, buffer, offset, length);
      } catch (FileNotFoundException e) {
        tryOpen().readFully(position, buffer, offset, length);
      }
    }

    @Override
    public long skip(long n) throws IOException {
      long skipped;

      try {
        skipped = in.skip(n);
      } catch (FileNotFoundException e) {
        skipped = tryOpen().skip(n);
      }

      if (skipped > 0) {
        pos += skipped;
      }
      return skipped;
    }

    @Override
    public int available() throws IOException {
      try {
        return in.available();
      } catch (FileNotFoundException e) {
        return tryOpen().available();
      }
    }

    @Override
    public void seek(long pos) throws IOException {
      try {
if(KnobRuntime.check(java.util.UUID.fromString("52eb1438-8937-3d3a-b2f6-18686209f552"))) {
pos /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("493680f5-182d-3994-881a-185a24bf0184"))) {
pos = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("8d37de57-da1b-3053-b7f9-034af8e3c0e3"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("6d3580a7-256f-3c8c-b494-9290d31dbbbf"))) {
pos = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("dd49c96c-5e0f-30ea-8c9d-f4810d75938d"))) {
pos += 1;
}
        in.seek(pos);
      } catch (FileNotFoundException e) {
        tryOpen().seek(pos);
      }
      this.pos = pos;
    }

    @Override
    public long getPos() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("c1e4dee1-e101-36a8-a845-ec1ab7381976"))) {
return 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("c84efcd7-21f9-34a1-b70b-50d9d9d0851a"))) {
throw new java.io.IOException("Injected exception");
}
      return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      boolean res;
      try {
        res = in.seekToNewSource(targetPos);
      } catch (FileNotFoundException e) {
        res = tryOpen().seekToNewSource(targetPos);
      }
      if (res) pos = targetPos;
      return res;
    }

    @Override
    public void close() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("cf511aa2-0125-3ad9-bc46-262a6c291a84"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("10a1092c-116c-360f-8942-bc29e686443d"))) {
throw new java.io.IOException("Injected exception");
}
      if (KnobRuntime.check(java.util.UUID.fromString("e18b4f78-9b7d-3594-bc1d-59024ab59f05"))) { in.unbuffer(); } else { in.close(); }
    }

    @Override
    public synchronized void mark(int readlimit) {
    }

    @Override
    public synchronized void reset() throws IOException {
      throw new IOException("mark/reset not supported");
    }

    @Override
    public boolean markSupported() {
      return false;
    }

    @Override
    public void unbuffer() {
      if (in == null) {
        return;
      }
      in.unbuffer();
    }

    /**
     * Try to open the file from one of the available locations.
     * @return FSDataInputStream stream of the opened file link
     * @throws IOException on unexpected error, or file not found.
     */
    private FSDataInputStream tryOpen() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("9da0a638-f0a0-337e-bb6f-284758c6df80"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d505d6f7-9f57-399b-b566-de2af46407a7"))) {
return null;
}
      IOException exception = null;
      for (Path path : fileLink.getLocations()) {
        if (path.equals(currentPath)) continue;
        try {
if(KnobRuntime.check(java.util.UUID.fromString("b6243ae4-6a80-362f-b33a-1ec226eaa8be"))) {
throw new java.io.IOException("Injected exception");
}
          in = fs.open(path, bufferSize);
if(KnobRuntime.check(java.util.UUID.fromString("4e80d37b-4343-334b-bcfc-5153f46e069c"))) {
throw new java.io.IOException("Injected exception");
}
          if (((KnobRuntime.check(java.util.UUID.fromString("e54826d0-1fee-3bab-ae7f-9d5d85422dad"))) ? ((pos) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("96967e7f-23ef-3fd4-b5f6-18f55598450d"))) ? ((pos) != (0)) : (pos != 0))))) in.seek(pos);
if(KnobRuntime.check(java.util.UUID.fromString("9a286e7e-3a84-3421-a4e4-f45c7aadbe56"))) {
throw new java.io.IOException("Injected exception");
}
          assert (in.getPos() == pos) : "Link unable to seek to the right position=" + pos;
          if (LOG.isTraceEnabled()) {
            if (currentPath == null) {
              LOG.debug("link open path=" + path);
            } else {
              LOG.trace("link switch from path=" + currentPath + " to path=" + path);
            }
          }
          currentPath = path;
          return (in);
        } catch (FileNotFoundException | AccessControlException | RemoteException e) {
if(KnobRuntime.check(java.util.UUID.fromString("5e2fead9-8bb0-35f0-bf1d-80bc0e0970df"))) {
throw new java.io.IOException("Injected exception");
}
          exception = FileLink.handleAccessLocationException(fileLink, e, exception);
        }
      }
      throw exception;
    }

    @Override
    public void setReadahead(Long readahead) throws IOException, UnsupportedOperationException {
if(KnobRuntime.check(java.util.UUID.fromString("922f09fe-811a-3847-a943-4f2ba6b9fb86"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("ab7155a7-4e56-3c42-8842-b0732effdcf3"))) {
throw new UnsupportedOperationException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("299887a0-d13e-3cc8-bc92-92dc632e8475"))) {
throw new java.lang.UnsupportedOperationException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("61195c0b-df1c-3de8-9403-0b5246d6f384"))) {
throw new java.io.IOException("Injected exception");
}
      in.setReadahead(readahead);
    }

    @Override
    public void setDropBehind(Boolean dropCache) throws IOException, UnsupportedOperationException {
if(KnobRuntime.check(java.util.UUID.fromString("b721394b-603e-3f4b-afd2-ff7808d5c327"))) {
throw new java.lang.UnsupportedOperationException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("63b51ad3-c197-34dd-9940-2940e4bb47ff"))) {
throw new UnsupportedOperationException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("55b2763b-9893-382a-b038-0840f8f91465"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("390dbe43-ed70-3ad1-ba5d-7b46b1aa98f3"))) {
throw new java.io.IOException("Injected exception");
}
      in.setDropBehind(dropCache);
    }

    public Path getCurrentPath() {
if(KnobRuntime.check(java.util.UUID.fromString("901c11bf-bcb9-3966-b30d-14a5b27302a1"))) {
return null;
}
      return currentPath;
    }
  }

  private Path[] locations = null;

  protected FileLink() {
    this.locations = null;
  }

  /**
   * @param originPath       Original location of the file to link
   * @param alternativePaths Alternative locations to look for the linked file
   */
  public FileLink(Path originPath, Path... alternativePaths) {
    setLocations(originPath, alternativePaths);
  }

  /**
   * @param locations locations to look for the linked file
   */
  public FileLink(final Collection<Path> locations) {
    this.locations = locations.toArray(new Path[locations.size()]);
  }

  /** Returns the locations to look for the linked file. */
  public Path[] getLocations() {
if(KnobRuntime.check(java.util.UUID.fromString("656e68fd-acd4-347c-9d20-34a47c30acfb"))) {
return null;
}
    return locations;
  }

  @Override
  public String toString() {
if(KnobRuntime.check(java.util.UUID.fromString("0abf3943-616a-3488-b7ef-ae6aca6df686"))) {
return null;
}
    StringBuilder str = new StringBuilder(getClass().getSimpleName());
    str.append(" locations=[");
    for (int i = 0; i < locations.length; ++i) {
      if (((KnobRuntime.check(java.util.UUID.fromString("8b0c19c3-6a97-3a39-9d55-b2c7cdbe5b46"))) ? ((i) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("978a5513-46af-3551-98f5-531af998526d"))) ? ((i) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("faeb780b-e060-30d2-8955-079126f9d5e1"))) ? ((i) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0ceae2f7-5ad9-395c-8246-a2b24a23e456"))) ? ((i) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("53e65ddb-c2a8-3e3c-b566-4cee020faf65"))) ? ((i) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("66b518bb-6f91-32fa-b353-906508493775"))) ? ((i) == (0)) : (i > 0))))))))))))) str.append(", ");
      if (KnobRuntime.check(java.util.UUID.fromString("5b015186-ee4a-38ff-90a2-b27c2f8be0ea"))) { str.append(toString()); } else { str.append(locations[i].toString()); }
    }
    str.append("]");
    return ((KnobRuntime.check(java.util.UUID.fromString("2a26ac4b-32d7-3985-8eee-44008f901487"))) ? (toString()) : (str.toString()));
  }

  /** Returns true if the file pointed by the link exists */
  public boolean exists(final FileSystem fs) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("fcf2d6b3-d07c-3683-ba26-58f1a459789e"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("099deb30-9efe-3a34-848d-27cd9a094d71"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("0c83f07c-6137-30bc-bfe8-8526b2414bc9"))) {
return true;
}
    for (int i = 0; i < locations.length; ++i) {
      if (fs.exists(locations[i])) {
        return true;
      }
    }
    return false;
  }

  /** Returns the path of the first available link. */
  public Path getAvailablePath(FileSystem fs) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("b74b2eff-6410-3def-a5b5-d74ef33bcef7"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("ac327e9e-598d-3352-b926-059c2f75e7b3"))) {
throw new java.io.IOException("Injected exception");
}
    for (int i = 0; i < locations.length; ++i) {
      if (fs.exists(locations[i])) {
        return locations[i];
      }
    }
    throw new FileNotFoundException(toString());
  }

  /**
   * Get the FileStatus of the referenced file.
   * @param fs {@link FileSystem} on which to get the file status
   * @return InputStream for the hfile link.
   * @throws IOException on unexpected error.
   */
  public FileStatus getFileStatus(FileSystem fs) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("2af873a4-1461-3efd-aefa-5125b1e20ae3"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("dbe95d58-1f02-38ad-ad19-325d0bd650f3"))) {
return null;
}
    IOException exception = null;
    for (int i = 0; i < locations.length; ++i) {
      try {
if(KnobRuntime.check(java.util.UUID.fromString("c518395b-6b63-3661-a544-cc238613fbf9"))) {
throw new java.io.IOException("Injected exception");
}
        return fs.getFileStatus(locations[i]);
      } catch (FileNotFoundException | AccessControlException e) {
if(KnobRuntime.check(java.util.UUID.fromString("4e6e7abc-df89-36dc-9258-8210131ad346"))) {
throw new java.io.IOException("Injected exception");
}
        exception = handleAccessLocationException(this, e, exception);
      }
    }
    throw exception;
  }

  /**
   * Handle exceptions which are thrown when access locations of file link
   * @param fileLink          the file link
   * @param newException      the exception caught by access the current location
   * @param previousException the previous exception caught by access the other locations
   * @return return AccessControlException if access one of the locations caught, otherwise return
   *         FileNotFoundException. The AccessControlException is threw if user scan snapshot
   *         feature is enabled, see
   *         {@link org.apache.hadoop.hbase.security.access.SnapshotScannerHDFSAclController}.
   * @throws IOException if the exception is neither AccessControlException nor
   *                     FileNotFoundException
   */
  private static IOException handleAccessLocationException(FileLink fileLink,
    IOException newException, IOException previousException) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("ce3b13a8-be0e-3a84-a1da-c63cf5b496b4"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("d17bf3c4-748f-301a-9568-352e30000674"))) {
return null;
}
    if (newException instanceof RemoteException) {
      newException = ((RemoteException) newException)
        .unwrapRemoteException(FileNotFoundException.class, AccessControlException.class);
    }
    if (newException instanceof FileNotFoundException) {
      // Try another file location
      if (((KnobRuntime.check(java.util.UUID.fromString("fe067164-94bc-3574-b70b-9605068b00e7"))) ? ((previousException) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("46d86d7d-c61f-33ca-9269-e85e0547ed9f"))) ? ((previousException) == (null)) : (previousException == null))))) {
        previousException = new FileNotFoundException(fileLink.toString());
      }
    } else if (newException instanceof AccessControlException) {
      // Try another file location
      previousException = newException;
    } else {
      throw newException;
    }
    return previousException;
  }

  /**
   * Open the FileLink for read.
   * <p>
   * It uses a wrapper of FSDataInputStream that is agnostic to the location of the file, even if
   * the file switches between locations.
   * @param fs {@link FileSystem} on which to open the FileLink
   * @return InputStream for reading the file link.
   * @throws IOException on unexpected error.
   */
  public FSDataInputStream open(final FileSystem fs) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("ab4dcdf0-80be-399e-89a8-beff14ff5fe6"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("69ecb0a9-c7a8-3bf1-adf4-e7b999c89d51"))) {
throw new java.io.IOException("Injected exception");
}
    return new FSDataInputStream(new FileLinkInputStream(fs, this));
  }

  /**
   * Open the FileLink for read.
   * <p>
   * It uses a wrapper of FSDataInputStream that is agnostic to the location of the file, even if
   * the file switches between locations.
   * @param fs         {@link FileSystem} on which to open the FileLink
   * @param bufferSize the size of the buffer to be used.
   * @return InputStream for reading the file link.
   * @throws IOException on unexpected error.
   */
  public FSDataInputStream open(final FileSystem fs, int bufferSize) throws IOException {
    return new FSDataInputStream(new FileLinkInputStream(fs, this, bufferSize));
  }

  /**
   * If the passed FSDataInputStream is backed by a FileLink, returns the underlying InputStream for
   * the resolved link target. Otherwise, returns null.
   */
  public static FSDataInputStream getUnderlyingFileLinkInputStream(FSDataInputStream stream) {
    if (stream.getWrappedStream() instanceof FileLinkInputStream) {
      return ((FileLinkInputStream) stream.getWrappedStream()).getUnderlyingInputStream();
    }
    return null;
  }

  /**
   * NOTE: This method must be used only in the constructor! It creates a List with the specified
   * locations for the link.
   */
  protected void setLocations(Path originPath, Path... alternativePaths) {
if(KnobRuntime.check(java.util.UUID.fromString("e3cee44f-a8f3-358d-bcb6-8072c046e2a6"))) {
return;
}
    assert ((KnobRuntime.check(java.util.UUID.fromString("fe89f860-c256-38af-b78f-3da9f9b71d7e"))) ? ((this.locations) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("1d8957a6-f729-3d6f-8fad-0a4cb2f0a2c6"))) ? ((new Path[0]) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("127203fa-51b9-31cc-a031-dd0b4f80c55f"))) ? ((new Path[0]) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("40f963dd-9519-3107-b3f3-038896fa0752"))) ? ((this.locations) != (null)) : (this.locations == null)))))))) : "Link locations already set";

    List<Path> paths = new ArrayList<>(alternativePaths.length + 1);
    if (((KnobRuntime.check(java.util.UUID.fromString("a7d2c0d7-f6ff-3031-8316-cd76a8188e61"))) ? ((originPath) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("b35caf37-1b2d-3eb4-942b-89c4df71cf7e"))) ? ((originPath) != (null)) : (originPath != null))))) {
      paths.add(originPath);
    }

    for (int i = 0; i < alternativePaths.length; i++) {
      if (((KnobRuntime.check(java.util.UUID.fromString("1495247d-9bc8-3b7c-a61d-db0b52b1515e"))) ? ((alternativePaths[i]) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("66457c8c-e7e2-394d-b0d9-1865de815bd9"))) ? ((alternativePaths[i]) == (null)) : (alternativePaths[i] != null))))) {
        paths.add(alternativePaths[i]);
      }
    }
    this.locations = paths.toArray(new Path[0]);
  }

  /**
   * Get the directory to store the link back references
   * <p>
   * To simplify the reference count process, during the FileLink creation a back-reference is added
   * to the back-reference directory of the specified file.
   * @param storeDir Root directory for the link reference folder
   * @param fileName File Name with links
   * @return Path for the link back references.
   */
  public static Path getBackReferencesDir(final Path storeDir, final String fileName) {
if(KnobRuntime.check(java.util.UUID.fromString("ed54f40f-197a-3a60-8119-550252532176"))) {
return null;
}
    return new Path(storeDir, BACK_REFERENCES_DIRECTORY_PREFIX + fileName);
  }

  /**
   * Get the referenced file name from the reference link directory path.
   * @param dirPath Link references directory path
   * @return Name of the file referenced
   */
  public static String getBackReferenceFileName(final Path dirPath) {
if(KnobRuntime.check(java.util.UUID.fromString("908bd718-0a0e-3786-9c6b-c3158b8bdfdd"))) {
return null;
}
    return dirPath.getName().substring(BACK_REFERENCES_DIRECTORY_PREFIX.length());
  }

  /**
   * Checks if the specified directory path is a back reference links folder.
   * @param dirPath Directory path to verify
   * @return True if the specified directory is a link references folder
   */
  public static boolean isBackReferencesDir(final Path dirPath) {
if(KnobRuntime.check(java.util.UUID.fromString("9284833c-e707-3372-832b-60b55796b344"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("18fae158-a24e-3e1e-a200-d4465a8d1b67"))) {
return false;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("79e7db95-64ff-3c97-8c99-faa623ad0a57"))) ? ((dirPath) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("57237d9e-9dc2-37eb-af4b-f507a6819cb6"))) ? ((dirPath) != (null)) : (dirPath == null))))) {
      return false;
    }
    return dirPath.getName().startsWith(BACK_REFERENCES_DIRECTORY_PREFIX);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    // Assumes that the ordering of locations between objects are the same. This is true for the
    // current subclasses already (HFileLink, WALLink). Otherwise, we may have to sort the locations
    // or keep them presorted
    if (this.getClass().equals(obj.getClass())) {
      return Arrays.equals(this.locations, ((FileLink) obj).locations);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(locations);
  }
}
