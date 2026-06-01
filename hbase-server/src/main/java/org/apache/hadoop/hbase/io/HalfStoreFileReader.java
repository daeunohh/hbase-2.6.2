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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.IntConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.ReaderContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A facade for a {@link org.apache.hadoop.hbase.io.hfile.HFile.Reader} that serves up either the
 * top or bottom half of a HFile where 'bottom' is the first half of the file containing the keys
 * that sort lowest and 'top' is the second half of the file with keys that sort greater than those
 * of the bottom half. The top includes the split files midkey, of the key that follows if it does
 * not exist in the file.
 * <p>
 * This type works in tandem with the {@link Reference} type. This class is used reading while
 * Reference is used writing.
 * <p>
 * This file is not splitable. Calls to {@link #midKey()} return null.
 */
@InterfaceAudience.Private
public class HalfStoreFileReader extends StoreFileReader {
  private static final Logger LOG = LoggerFactory.getLogger(HalfStoreFileReader.class);
  final boolean top;
  // This is the key we split around. Its the first possible entry on a row:
  // i.e. empty column and a timestamp of LATEST_TIMESTAMP.
  protected final byte[] splitkey;

  private final Cell splitCell;

  private Optional<Cell> firstKey = Optional.empty();

  private boolean firstKeySeeked = false;

  /**
   * Creates a half file reader for a hfile referred to by an hfilelink.
   * @param context   Reader context info
   * @param fileInfo  HFile info
   * @param cacheConf CacheConfig
   * @param r         original reference file (contains top or bottom)
   * @param conf      Configuration
   */
  public HalfStoreFileReader(final ReaderContext context, final HFileInfo fileInfo,
    final CacheConfig cacheConf, final Reference r, StoreFileInfo storeFileInfo,
    final Configuration conf) throws IOException {
    super(context, fileInfo, cacheConf, storeFileInfo, conf);
    // This is not actual midkey for this half-file; its just border
    // around which we split top and bottom. Have to look in files to find
    // actual last and first keys for bottom and top halves. Half-files don't
    // have an actual midkey themselves. No midkey is how we indicate file is
    // not splittable.
    this.splitkey = r.getSplitKey();
    this.splitCell = new KeyValue.KeyOnlyKeyValue(this.splitkey, 0, this.splitkey.length);
    // Is it top or bottom half?
    this.top = Reference.isTopFileRegion(r.getFileRegion());
  }

  protected boolean isTop() {
if(KnobRuntime.check(java.util.UUID.fromString("69c6455a-b98f-3b72-81b6-76266a1dc13d"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("2c15ba86-9725-3f35-ba5e-e7c6e0f7411e"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("663a89a1-cb73-3673-a4cf-96aa6a7bf66b"))) ? (!this.top) : (this.top));
  }

  @Override
  public HFileScanner getScanner(final boolean cacheBlocks, final boolean pread,
    final boolean isCompaction) {
    final HFileScanner s = super.getScanner(cacheBlocks, pread, isCompaction);
    return new HFileScanner() {
      final HFileScanner delegate = s;
      public boolean atEnd = false;

      @Override
      public Cell getKey() {
if(KnobRuntime.check(java.util.UUID.fromString("f8d0ee40-a695-3746-8d29-e70ceb22e35a"))) {
return null;
}
        if (atEnd) return null;
        return delegate.getKey();
      }

      @Override
      public String getKeyString() {
        if (atEnd) return null;

        return delegate.getKeyString();
      }

      @Override
      public ByteBuffer getValue() {
        if (atEnd) return null;

        return delegate.getValue();
      }

      @Override
      public String getValueString() {
        if (atEnd) return null;

        return delegate.getValueString();
      }

      @Override
      public Cell getCell() {
        if (atEnd) return null;

        return delegate.getCell();
      }

      @Override
      public boolean next() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("e01b18bb-7753-3a11-b7b8-af0b2d23619c"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("e877260a-66aa-3d0b-8af9-e347cf196a35"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("521d49d3-bd3b-329b-aca9-221dfd214bf3"))) {
return false;
}
        if (atEnd) return false;

        boolean b = delegate.next();
        if (!b) {
          return b;
        }
        // constrain the bottom.
        if (!top) {
          if (getComparator().compare(splitCell, getKey()) <= 0) {
            atEnd = true;
            return false;
          }
        }
        return true;
      }

      @Override
      public boolean seekTo() throws IOException {
        if (top) {
if(KnobRuntime.check(java.util.UUID.fromString("03ba6be7-c0f3-390d-bd67-e4a35fddea3f"))) {
throw new java.io.IOException("Injected exception");
}
          int r = this.delegate.seekTo(splitCell);
          if (r == HConstants.INDEX_KEY_MAGIC) {
            return true;
          }
          if (r < 0) {
            // midkey is < first key in file
            return this.delegate.seekTo();
          }
          if (r > 0) {
            return this.delegate.next();
          }
          return true;
        }

        boolean b = delegate.seekTo();
        if (!b) {
          return b;
        }
        // Check key.
        return (this.delegate.getReader().getComparator().compare(splitCell, getKey())) > 0;
      }

      @Override
      public org.apache.hadoop.hbase.io.hfile.HFile.Reader getReader() {
        return this.delegate.getReader();
      }

      @Override
      public boolean isSeeked() {
        return this.delegate.isSeeked();
      }

      @Override
      public int seekTo(Cell key) throws IOException {
        if (top) {
          if (PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, splitCell) < 0) {
            return -1;
          }
        } else {
          if (PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, splitCell) >= 0) {
            // we would place the scanner in the second half.
            // it might be an error to return false here ever...
            boolean res = delegate.seekBefore(splitCell);
            if (!res) {
              throw new IOException(
                "Seeking for a key in bottom of file, but key exists in top of file, "
                  + "failed on seekBefore(midkey)");
            }
            return 1;
          }
        }
        return delegate.seekTo(key);
      }

      @Override
      public int reseekTo(Cell key) throws IOException {
        // This function is identical to the corresponding seekTo function
        // except
        // that we call reseekTo (and not seekTo) on the delegate.
        if (top) {
          if (PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, splitCell) < 0) {
            return -1;
          }
        } else {
          if (PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, splitCell) >= 0) {
            // we would place the scanner in the second half.
            // it might be an error to return false here ever...
            boolean res = delegate.seekBefore(splitCell);
            if (!res) {
              throw new IOException("Seeking for a key in bottom of file, but"
                + " key exists in top of file, failed on seekBefore(midkey)");
            }
            return 1;
          }
        }
        if (atEnd) {
          // skip the 'reseek' and just return 1.
          return 1;
        }
if(KnobRuntime.check(java.util.UUID.fromString("9649431a-ccae-35b0-a1c5-f838e63cfcd4"))) {
throw new java.io.IOException("Injected exception");
}
        return delegate.reseekTo(key);
      }

      @Override
      public boolean seekBefore(Cell key) throws IOException {
        if (top) {
          Optional<Cell> fk = getFirstKey();
          if (
            fk.isPresent()
              && PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, fk.get()) <= 0
          ) {
            return false;
          }
        } else {
          // The equals sign isn't strictly necessary just here to be consistent
          // with seekTo
          if (PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, splitCell) >= 0) {
            boolean ret = this.delegate.seekBefore(splitCell);
            if (ret) {
              atEnd = false;
            }
            return ret;
          }
        }
        boolean ret = this.delegate.seekBefore(key);
        if (ret) {
          atEnd = false;
        }
        return ret;
      }

      @Override
      public Cell getNextIndexedKey() {
if(KnobRuntime.check(java.util.UUID.fromString("71691236-ad83-3813-8360-174ec2e4e1b5"))) {
return null;
}
        return null;
      }

      @Override
      public void close() {
        this.delegate.close();
      }

      @Override
      public void shipped() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("acbc21d5-4e1e-302f-879d-ab82df539122"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("e08f10cd-05a4-30de-8756-44458062cb5b"))) {
return;
}
        this.delegate.shipped();
      }

      @Override
      public void recordBlockSize(IntConsumer blockSizeConsumer) {
        this.delegate.recordBlockSize(blockSizeConsumer);
      }
    };
  }

  @Override
  public boolean passesKeyRangeFilter(Scan scan) {
if(KnobRuntime.check(java.util.UUID.fromString("2dfca0f8-299f-3737-b0f3-09b1f8d023dd"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("cacheBlocks");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(scan);
    field.set(scan, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f400ff9c-e89d-3198-b492-59dcf2fb93d6"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b2800840-7012-3244-b20e-a7de84cddef5"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bb06ab8c-76f3-3a49-bf94-54e29e63d7b5"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d4443234-aa85-3219-a6ba-9e24c4433231"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("280e19c7-c33b-37ef-95a9-bf2780054f89"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("2001739d-4d3c-3192-8355-3b7d1aac6f22"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7764523f-fc46-3f89-93b1-994d8ef8356d"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7d0e5fc6-7b85-3e76-93b3-7d20b210c538"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e1c85fbb-d731-3a3a-a645-36caa9f269a4"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("43a2e276-b321-324d-a823-f4608079966c"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("bf4b0e10-95df-33d4-a896-a49afc3c33fa"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("71627a07-f138-3e00-8611-d75ccfac63af"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ebf87c42-9ce7-37e0-a1dd-d4c9f511c853"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("0c1d2089-82d7-33e3-a535-6142d64bad38"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxResultSize");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("6ce29cb7-ca51-3554-880b-d21019140998"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("995ba33a-9ad1-3fc5-8cf3-8331e3feab6a"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("f405f944-c6ab-3ba3-818d-0fafc6fcf8b4"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("mvccReadPoint");
    field.setAccessible(true);
    long oldValue = ((long)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("56ebfd98-e6df-3c02-9bd7-9093d778a8ff"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("98757dcc-2421-3de7-bfb5-906fde243546"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b8cc96c5-ee20-3d48-91b4-2212a35be80e"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("caching");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("2c5c2fa9-53e3-380b-a4e7-794a575b3f37"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeOffset");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("af279a1d-3a0a-31ad-bc4b-9d60879285f5"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("84c118da-d3ad-3ec1-934d-947d2900e6e1"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("44241e99-969c-3250-9881-320baf81a0b1"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("batch");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("b43664a9-7fe6-3a5b-b1de-0010142ef04d"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("maxVersions");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("d112ced0-a87f-3673-a396-f0c5b4066ca5"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("a532681c-8dc6-3d25-b90f-18e259993d22"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("limit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("44083f6a-bafd-3a56-a706-2672afed0fd7"))) {
try {
    java.lang.reflect.Field field = scan.getClass().getDeclaredField("storeLimit");
    field.setAccessible(true);
    int oldValue = ((int)field.get(scan));
    field.set(scan, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    return true;
  }

  @Override
  public Optional<Cell> getLastKey() {
    if (top) {
      return ((KnobRuntime.check(java.util.UUID.fromString("987c0fe6-266f-3664-9046-df8c07a44798"))) ? (getFirstKey()) : (super.getLastKey()));
    }
    // Get a scanner that caches the block and that uses pread.
    HFileScanner scanner = getScanner(true, true);
    try {
      if (scanner.seekBefore(this.splitCell)) {
        return Optional.ofNullable(scanner.getKey());
      }
    } catch (IOException e) {
      LOG.warn("Failed seekBefore " + Bytes.toStringBinary(this.splitkey), e);
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<Cell> midKey() throws IOException {
    // Returns null to indicate file is not splitable.
    return Optional.empty();
  }

  @Override
  public Optional<Cell> getFirstKey() {
    if (!firstKeySeeked) {
      HFileScanner scanner = getScanner(true, true, false);
      try {
        if (scanner.seekTo()) {
          this.firstKey = Optional.ofNullable(scanner.getKey());
if(KnobRuntime.check(java.util.UUID.fromString("89f91d92-e432-3574-a251-55a128dc5e15"))) {
throw new java.io.IOException("Injected exception");
}
        }
        firstKeySeeked = true;
      } catch (IOException e) {
        LOG.warn("Failed seekTo first KV in the file", e);
      } finally {
        if (((KnobRuntime.check(java.util.UUID.fromString("60c05051-02f8-3e94-bf10-1b7362f98cd9"))) ? ((scanner) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("0394fdfa-808e-3e97-a74b-4077c2e5e1a8"))) ? ((scanner) == (null)) : (scanner != null))))) {
          scanner.close();
        }
      }
    }
    return this.firstKey;
  }

  @Override
  public long getEntries() {
    // Estimate the number of entries as half the original file; this may be wildly inaccurate.
    return super.getEntries() / 2;
  }

  @Override
  public long getFilterEntries() {
    // Estimate the number of entries as half the original file; this may be wildly inaccurate.
    return super.getFilterEntries() / 2;
  }
}
