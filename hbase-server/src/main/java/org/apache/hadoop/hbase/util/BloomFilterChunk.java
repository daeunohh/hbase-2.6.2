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
package org.apache.hadoop.hbase.util;
import org.knobinjection.runtime.KnobRuntime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The basic building block for the {@link org.apache.hadoop.hbase.io.hfile.CompoundBloomFilter}
 */
@InterfaceAudience.Private
public class BloomFilterChunk implements BloomFilterBase {

  /** Bytes (B) in the array. This actually has to fit into an int. */
  protected long byteSize;
  /** Number of hash functions */
  protected int hashCount;
  /** Hash type */
  protected final int hashType;
  /** Hash Function */
  protected final Hash hash;
  /** Keys currently in the bloom */
  protected int keyCount;
  /** Max Keys expected for the bloom */
  protected int maxKeys;
  /** Bloom bits */
  protected ByteBuffer bloom;
  /** The type of bloom */
  protected BloomType bloomType;

  /**
   * Loads bloom filter meta data from file input.
   * @param meta stored bloom meta data
   * @throws IllegalArgumentException meta data is invalid
   */
  public BloomFilterChunk(DataInput meta) throws IOException, IllegalArgumentException {
    this.byteSize = meta.readInt();
    this.hashCount = meta.readInt();
    this.hashType = meta.readInt();
    this.keyCount = meta.readInt();
    this.maxKeys = this.keyCount;

    this.hash = Hash.getInstance(this.hashType);
    if (hash == null) {
      throw new IllegalArgumentException("Invalid hash type: " + hashType);
    }
    sanityCheck();
  }

  /**
   * Computes the error rate for this Bloom filter, taking into account the actual number of hash
   * functions and keys inserted. The return value of this function changes as a Bloom filter is
   * being populated. Used for reporting the actual error rate of compound Bloom filters when
   * writing them out.
   * @return error rate for this particular Bloom filter
   */
  public double actualErrorRate() {
    return BloomFilterUtil.actualErrorRate(keyCount, byteSize * 8, hashCount);
  }

  public BloomFilterChunk(int hashType, BloomType bloomType) {
    this.hashType = hashType;
    this.hash = Hash.getInstance(hashType);
    this.bloomType = bloomType;
  }

  /**
   * Determines &amp; initializes bloom filter meta data from user config. Call
   * {@link #allocBloom()} to allocate bloom filter data.
   * @param maxKeys    Maximum expected number of keys that will be stored in this bloom
   * @param errorRate  Desired false positive error rate. Lower rate = more storage required
   * @param hashType   Type of hash function to use
   * @param foldFactor When finished adding entries, you may be able to 'fold' this bloom to save
   *                   space. Tradeoff potentially excess bytes in bloom for ability to fold if
   *                   keyCount is exponentially greater than maxKeys.
   */
  // Used only in testcases
  public BloomFilterChunk(int maxKeys, double errorRate, int hashType, int foldFactor)
    throws IllegalArgumentException {
    this(hashType, BloomType.ROW);

    long bitSize = BloomFilterUtil.computeBitSize(maxKeys, errorRate);
    hashCount = BloomFilterUtil.optimalFunctionCount(maxKeys, bitSize);
    this.maxKeys = maxKeys;

    // increase byteSize so folding is possible
    byteSize = BloomFilterUtil.computeFoldableByteSize(bitSize, foldFactor);

    sanityCheck();
  }

  /**
   * Creates another similar Bloom filter. Does not copy the actual bits, and sets the new filter's
   * key count to zero.
   * @return a Bloom filter with the same configuration as this
   */
  public BloomFilterChunk createAnother() {
    BloomFilterChunk bbf = new BloomFilterChunk(hashType, this.bloomType);
    bbf.byteSize = byteSize;
    bbf.hashCount = hashCount;
    bbf.maxKeys = maxKeys;
    return bbf;
  }

  public void allocBloom() {
if(KnobRuntime.check(java.util.UUID.fromString("292b6758-c83f-3137-b539-179512556113"))) {
return;
}
    if (((KnobRuntime.check(java.util.UUID.fromString("e7917583-c3cb-3360-bfa7-f3eff20a9c99"))) ? ((this.bloom) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("43376ac1-b2a6-3fd9-8dcb-cd580fc57c6a"))) ? ((this.bloom) != (null)) : (this.bloom != null))))) {
      throw new IllegalArgumentException("can only create bloom once.");
    }
    this.bloom = ByteBuffer.allocate((int) this.byteSize);
    assert this.bloom.hasArray();
  }

  void sanityCheck() throws IllegalArgumentException {
    if (0 >= this.byteSize || this.byteSize > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Invalid byteSize: " + this.byteSize);
    }

    if (this.hashCount <= 0) {
      throw new IllegalArgumentException("Hash function count must be > 0");
    }

    if (this.hash == null) {
      throw new IllegalArgumentException("hashType must be known");
    }

    if (this.keyCount < 0) {
      throw new IllegalArgumentException("must have positive keyCount");
    }
  }

  void bloomCheck(ByteBuffer bloom) throws IllegalArgumentException {
    if (this.byteSize != bloom.limit()) {
      throw new IllegalArgumentException("Configured bloom length should match actual length");
    }
  }

  // Used only by tests
  void add(byte[] buf, int offset, int len) {
    /*
     * For faster hashing, use combinatorial generation
     * http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
     */
    HashKey<byte[]> hashKey = new ByteArrayHashKey(buf, offset, len);
    int hash1 = this.hash.hash(hashKey, 0);
    int hash2 = this.hash.hash(hashKey, hash1);
    setHashLoc(hash1, hash2);
  }

  public void add(Cell cell) {
    /*
     * For faster hashing, use combinatorial generation
     * http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
     */
    int hash1;
    int hash2;
    HashKey<Cell> hashKey;
    if (((KnobRuntime.check(java.util.UUID.fromString("609f8c15-7095-38a0-9acd-ab06a83aaeda"))) ? ((this.bloomType) != (BloomType.ROWCOL)) : (((KnobRuntime.check(java.util.UUID.fromString("f36faae3-d9b7-395b-a730-a715fb47f8d6"))) ? ((this.bloomType) == (BloomType.ROWCOL)) : (this.bloomType == BloomType.ROWCOL))))) {
      hashKey = new RowColBloomHashKey(cell);
      hash1 = this.hash.hash(hashKey, 0);
if(KnobRuntime.check(java.util.UUID.fromString("db910e95-c5d5-3a82-97b7-7cd7c04d4534"))) {
hash1 = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1243f099-746d-3242-b967-ae75680e7d7a"))) {
hash1 = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("5f79bb4f-7e4f-3dc4-b251-e7570d0ca15d"))) {
hash1 *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("9cbc7e82-78f6-3161-bccb-fe8ccfa41996"))) {
hash1 += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("6b253589-4820-3212-9872-3bef6308b9dc"))) {
hash1 /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("67c8cdf4-3e89-38d8-ae18-e95e7c06e9b7"))) {
hash1 -= 1;
}
      hash2 = this.hash.hash(hashKey, hash1);
    } else {
      hashKey = new RowBloomHashKey(cell);
      hash1 = this.hash.hash(hashKey, 0);
if(KnobRuntime.check(java.util.UUID.fromString("f411d86b-815b-36ca-aaa2-6f05fdd3cc25"))) {
hash1 *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("b24b50f0-e288-38a1-8219-53e26d25a28c"))) {
hash1 /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("d0514d02-7298-3aad-a489-4fb33493c3c9"))) {
hash1 = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b32696ec-dbb1-39d0-b3e6-f6c1ca27f92b"))) {
hash1 += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("91bb5b3d-41bc-3902-b9f2-b470b28baa88"))) {
hash1 = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("8beaa2c0-c42b-3d74-a1c5-db4d9dae4e5e"))) {
hash1 -= 1;
}
      hash2 = this.hash.hash(hashKey, hash1);
    }
if(KnobRuntime.check(java.util.UUID.fromString("7623e2ae-1f7e-3cff-be55-540b48113b98"))) {
hash2 *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("105201a8-2153-3445-a979-8eb076dc1ae6"))) {
hash2 = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("16af8998-44bf-3dd1-9a52-0b45f9fc918f"))) {
hash1 = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("377b4274-ee0d-3dd5-8e8a-ee98bcd0f0e1"))) {
hash1 = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("707a2588-0030-3165-8c1f-12b3a502f8b4"))) {
hash1 /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("b5e91a83-84c4-3179-a518-23ef1e52f1e8"))) {
hash2 = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("1ec8757b-8dbf-3788-88db-9012024d02f5"))) {
hash2 += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e74232ad-f056-3dd9-8e0e-5db1126278e5"))) {
hash1 -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("935bf67d-ce60-370f-a6f4-259eb1ad2b2b"))) {
hash2 /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("ba0d92ec-b24d-336d-99eb-88f9bf724dc7"))) {
hash1 += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("7d6a860b-609c-3247-aa59-857e46cdbb06"))) {
hash2 -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("db11e5d6-dc32-3662-bb45-3e70038969e2"))) {
hash1 *= 2;
}
    if (KnobRuntime.check(java.util.UUID.fromString("6daeffef-06cc-3a5e-8a86-5cd34c24ad56"))) { setHashLoc(hash1, 0); } else if (KnobRuntime.check(java.util.UUID.fromString("f3cc2ea0-0db7-3ad8-916c-8b9feb72f6f6"))) { setHashLoc(0, hash2); } else { setHashLoc(hash1, hash2); }
  }

  private void setHashLoc(int hash1, int hash2) {
if(KnobRuntime.check(java.util.UUID.fromString("3f24cec9-2ead-3b74-b025-b0cde83377ae"))) {
return;
}
    for (int i = 0; i < this.hashCount; i++) {
      long hashLoc = Math.abs((hash1 + i * hash2) % (this.byteSize * 8));
if(KnobRuntime.check(java.util.UUID.fromString("2274f8df-aeae-30cc-a10e-8ee12aeca88a"))) {
hashLoc += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c4aa8387-533d-3ab7-ab39-a6e3e6e43530"))) {
hashLoc = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("fdf41245-b175-328e-b91a-5980c8485eba"))) {
hashLoc = 0;
}
      set(hashLoc);
    }

    ++this.keyCount;
  }

  // ---------------------------------------------------------------------------
  /** Private helpers */

  /**
   * Set the bit at the specified index to 1.
   * @param pos index of bit
   */
  void set(long pos) {
if(KnobRuntime.check(java.util.UUID.fromString("6302f958-796f-3ed3-b0e9-0088f997e4ea"))) {
return;
}
    int bytePos = (int) (pos / 8);
    int bitPos = (int) (pos % 8);
if(KnobRuntime.check(java.util.UUID.fromString("ea97b5d3-6a6d-35ab-8a5a-9dce85f717e9"))) {
bytePos = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("cc7dbd03-325b-3fc3-9696-4ee3a8762731"))) {
bytePos += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c57833f1-297c-367f-a717-f2e2be6d6068"))) {
bytePos = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("bb211ff2-45ed-3795-9afc-901008214fc9"))) {
bytePos -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("b21bde0a-22b7-3379-8c93-e58ae77e6f17"))) {
bytePos *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("44b0f43d-8635-3de2-ae4f-2a0b7b0e51cc"))) {
bytePos /= 2;
}
    byte curByte = bloom.get(bytePos);
    curByte |= BloomFilterUtil.bitvals[bitPos];
if(KnobRuntime.check(java.util.UUID.fromString("9b84a0e8-fc23-39cc-a04a-dd631b488253"))) {
bytePos -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("61da6be6-70ca-3079-97ef-1f0e63b4ed2e"))) {
curByte = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("0a1d30f2-1cc6-3641-9c48-64cbc74deb20"))) {
bytePos = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9f5786c9-83c8-3ae0-abb7-28defe940a4e"))) {
bytePos = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("c58bc583-812c-3219-9b98-75730deef7e9"))) {
bytePos += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d868fea5-60cc-38c6-821a-6316d68c3570"))) {
bytePos *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("b3ee5349-7cf1-3804-bf5f-7c15991f8d72"))) {
bytePos /= 2;
}
    if (KnobRuntime.check(java.util.UUID.fromString("37104627-7b8e-314a-beb0-0a900dcaac39"))) { bloom.put(8, curByte); } else { bloom.put(bytePos, curByte); }
  }

  /**
   * Check if bit at specified index is 1.
   * @param pos index of bit
   * @return true if bit at specified index is 1, false if 0.
   */
  static boolean get(int pos, ByteBuffer bloomBuf, int bloomOffset) {
    int bytePos = pos >> 3; // pos / 8
    int bitPos = pos & 0x7; // pos % 8
    // TODO access this via Util API which can do Unsafe access if possible(?)
    byte curByte = bloomBuf.get(bloomOffset + bytePos);
    curByte &= BloomFilterUtil.bitvals[bitPos];
    return (curByte != 0);
  }

  @Override
  public long getKeyCount() {
if(KnobRuntime.check(java.util.UUID.fromString("c2539c7e-77fe-302c-b716-633a7f51ae80"))) {
return 0;
}
    return keyCount;
  }

  @Override
  public long getMaxKeys() {
if(KnobRuntime.check(java.util.UUID.fromString("2b409af2-3244-3b5e-ab0e-1de82c5689bf"))) {
return 0;
}
    return maxKeys;
  }

  @Override
  public long getByteSize() {
if(KnobRuntime.check(java.util.UUID.fromString("a1f732ed-df41-338f-853d-0ed59f41528d"))) {
return 0;
}
    return byteSize;
  }

  public int getHashType() {
if(KnobRuntime.check(java.util.UUID.fromString("9774e68f-18c8-3aba-a73a-259cb72b9d8f"))) {
return 0;
}
    return hashType;
  }

  public void compactBloom() {
    // see if the actual size is exponentially smaller than expected.
    if (((KnobRuntime.check(java.util.UUID.fromString("d7bacfe7-dfeb-3f76-97c6-6d26503bd9b4"))) ? (((this.keyCount) < (0)) || (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("1458812f-a1ed-3645-af6a-94e060732c63"))) ? (((this.keyCount << 1) < (0)) && (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("32701457-4a04-3529-9180-2cd5878c4f06"))) ? (((this.keyCount << 1) < (0)) || (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("01bb615c-04d8-376e-b6ee-27de935a7ded"))) ? ((this.keyCount << 1) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("86372908-5ff4-3ce2-b1cb-2ef04cea93c1"))) ? (((this.keyCount << 1) == (0)) || (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("6e364bda-a095-3947-add7-f161a7bf70f1"))) ? (((this.keyCount << 1) >= (0)) || (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("c0e6c808-f138-364c-a40b-47cc13836970"))) ? (((this.keyCount << 1) == (0)) && (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("8e20870e-51bd-3135-9b3b-ec96749652ce"))) ? (((this.keyCount << 1) != (0)) || (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("c43bb0f5-e0e3-38ac-a2a2-7c0fd3bc38bf"))) ? (((this.keyCount << 1)) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("a65e5fc1-0b6e-3740-aad3-e8672f089c82"))) ? ((this.keyCount > 0) || (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("2ae272d5-51dd-3ca1-9694-0fac37fc115f"))) ? ((((this.keyCount << 1)) <= (0)) || (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("90b583d1-2cc9-3de3-b89f-8d91a2e8fa0a"))) ? ((this.keyCount << 1) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1ddfec2f-7cfd-3c60-a7ae-b0903431aeda"))) ? (((this.keyCount << 1)) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bdd262c8-53b8-30ed-a89a-af025854e190"))) ? ((this.keyCount) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("32456c20-2e6c-3644-97f1-98fffc7a9ef2"))) ? ((this.keyCount << 1) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("f899d1b9-fbc6-3e42-b669-1ef0c6d3ccb7"))) ? (((this.keyCount << 1) <= (0)) || (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("27c345fc-497d-3673-9f3f-d4d8cd53a463"))) ? (((this.keyCount << 1)) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("72e151d9-730b-3364-83b8-90acb2408a97"))) ? (((this.keyCount << 1) <= (0)) && (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("9d5c78ea-1a25-32d0-aadb-a7d278b77ec6"))) ? ((((this.keyCount << 1)) < (0)) || (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("798364f6-cf04-350b-af00-63a59e1b1409"))) ? (((this.keyCount) != (0)) && (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("769da7c4-db2c-339e-bdd5-8eb93fd48497"))) ? ((((this.keyCount << 1)) != (0)) || (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("2ac63d6f-b08e-3fb2-b860-56e066d1f0fe"))) ? ((((this.keyCount << 1)) > (0)) && (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("99334af6-bffc-316a-84cd-62c36e5ed84e"))) ? (this.keyCount > 0) : (((KnobRuntime.check(java.util.UUID.fromString("f81448a7-76fa-3b4c-9191-d296d7562e6e"))) ? ((this.keyCount) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("373f9515-06dc-3280-99d7-85a18ca7499c"))) ? ((this.keyCount << 1) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0c0efada-8542-3970-88d6-570e1f765e79"))) ? ((this.keyCount) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("96faf831-9068-3161-b121-c79975e0bbda"))) ? ((((this.keyCount << 1)) == (0)) || (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("c52b2834-1151-3dc4-bd84-33c4f029964a"))) ? (((this.keyCount) == (0)) && (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("bcc0caf0-f666-3144-9215-2558a8122568"))) ? ((this.keyCount << 1) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("63dd2a3e-1caf-3adc-abd1-928d669f3227"))) ? ((((this.keyCount << 1)) >= (0)) && (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("75a1fd23-3ea5-3669-be12-d31fb80fcc0c"))) ? (this.bloom.hasArray()) : (((KnobRuntime.check(java.util.UUID.fromString("12578c2d-9272-3bed-a9e2-4c48368bbba7"))) ? ((this.keyCount << 1) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("15e0c3c7-4b8d-3569-9538-0b1ec406466f"))) ? (((this.keyCount << 1)) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("15cf7cd5-3e86-300a-93c0-f43d6fc03a5a"))) ? (((this.keyCount) != (0)) || (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("51a45ad1-af63-3070-aafe-835c692bcfa8"))) ? ((((this.keyCount << 1)) != (0)) && (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("a08d4df4-5ba7-351a-95ca-5bbac4763593"))) ? (((this.keyCount) > (0)) || (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("0177320f-99e8-3250-a81b-8d66bde8ea2f"))) ? ((((this.keyCount << 1)) >= (0)) || (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("9a4976e4-f6c8-3cb6-a20b-2d759e5e1c0e"))) ? (((this.keyCount) == (0)) || (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("8bcdd264-a40c-37ce-bbe7-c003d5022215"))) ? (((this.keyCount << 1) > (0)) || (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("d3137d8e-178a-3ec1-af55-84c2339a29cf"))) ? (((this.keyCount) >= (0)) || (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("291a6caa-bed5-3533-8030-f25c3a35920e"))) ? (((this.keyCount << 1)) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("78b372ba-6b0b-37d9-ba10-5d6d3dee0207"))) ? (((this.keyCount << 1) > (0)) && (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("d4c0d07f-8edf-3857-9e99-0863e595dca6"))) ? (((this.keyCount) <= (0)) || (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("0e2d6d40-11ed-3f14-ae77-2902c8b5c7b4"))) ? (((this.keyCount << 1) != (0)) && (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("e89ab946-c1e6-3a2a-bdc9-c8efca91adcd"))) ? ((((this.keyCount << 1)) == (0)) && (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("02c5b119-756b-361d-8018-1e04de73338d"))) ? ((this.keyCount > 0) && (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("970312cc-88b5-3772-9969-e97beaf072a8"))) ? (((this.keyCount << 1) >= (0)) && (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("2c93db2b-294c-3f57-a71f-922c98b5706d"))) ? ((((this.keyCount << 1)) <= (0)) && (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("41d12705-ab88-3751-af69-8339cec56d94"))) ? ((this.keyCount) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("0ef31da8-0d09-38cf-9ecd-620ef3a69958"))) ? ((((this.keyCount << 1)) > (0)) || (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("2b04ee78-f935-3190-9ec8-3f884afd1c1d"))) ? (((this.keyCount << 1)) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("ea5d7934-f1dc-38ee-8e19-9bc72321a208"))) ? (((this.keyCount) > (0)) && (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("e3f44e16-4071-366f-8c7e-6d8bc7f0c635"))) ? ((this.keyCount) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("1ad66dcb-193d-3891-97aa-ddbca0aeb059"))) ? ((((this.keyCount << 1)) < (0)) && (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("fc63135f-c560-3bab-b471-15a00b1a047b"))) ? (((this.keyCount) >= (0)) && (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("f09ba6f9-efef-39e1-ba22-0047425c2544"))) ? (((this.keyCount) < (0)) && (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("a1f76de5-2e2d-3aaa-878c-6bd795b5fe06"))) ? (((this.keyCount) <= (0)) && (this.bloom.hasArray())) : (((KnobRuntime.check(java.util.UUID.fromString("3d8822d0-a4c9-3752-ad52-39130787e925"))) ? ((this.keyCount) < (0)) : (this.keyCount > 0 && this.bloom.hasArray()))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      int pieces = 1;
      int newByteSize = (int) this.byteSize;
      int newMaxKeys = this.maxKeys;

      // while exponentially smaller & folding is lossless
      while (((KnobRuntime.check(java.util.UUID.fromString("89b8852a-19ee-3b8d-b9f8-e17945d777dd"))) ? ((1) == ((this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("d219425d-42df-3938-a617-b28a547379d0"))) ? ((0) >= ((this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("ebb6876f-4bb9-3986-bf37-252af3391e0b"))) ? (((newByteSize & 1) == 0) && ((newMaxKeys) == ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("a36707e7-48ee-3357-a6bc-0fc513d8df08"))) ? ((((newByteSize & 1)) == (0)) || ((newMaxKeys) != ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("c21dbf63-0aa1-379a-a015-d97e34f93e40"))) ? (((newByteSize & 1) == 0) && ((0) >= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("859e521a-8cb7-3c40-a98b-c4f808ba3d82"))) ? ((newMaxKeys) >= ((this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("4ab39399-90f7-303d-a512-a9917e0620b0"))) ? ((((newByteSize & 1)) != (0)) || ((1) != ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("070a8a52-46ed-38d8-a16a-02d9da12cd9f"))) ? ((((newByteSize & 1)) == (0)) && ((0) > ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("9dafcae2-9c6d-33aa-8fdc-07d1f91e00ea"))) ? ((((newByteSize & 1)) != (0)) || ((newMaxKeys) < ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("aead0d4e-bbde-393f-b88f-736a13e56936"))) ? ((newByteSize & 1) == 0) : (((KnobRuntime.check(java.util.UUID.fromString("86502802-d3c6-319d-b2d9-295517f0dea3"))) ? (((newByteSize & 1) == 0) && ((newMaxKeys) > ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("58273523-ce3c-3a41-bab7-299ab69d9c41"))) ? ((((newByteSize & 1)) == (0)) && ((0) != ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("26a88190-8aa6-3c1f-9eb0-308dbe7595e2"))) ? ((newMaxKeys) <= ((this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("1a0b741a-ab96-3aec-be05-b4f6130de86e"))) ? ((((newByteSize & 1)) == (0)) || (newMaxKeys > (this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("ce6e43d0-275d-3c8a-a94d-950335073a7c"))) ? (((newByteSize & 1) == 0) && ((newMaxKeys) < ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("d7cb1d44-ed85-33f4-809b-4452a05c9235"))) ? ((1) <= ((this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("77f24cd9-4734-3085-a6bf-d6c33498df55"))) ? (((newByteSize & 1) == 0) && ((0) < ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("7dd81f75-5fda-306b-9d4a-c6cb2e95720a"))) ? ((((newByteSize & 1)) != (0)) || ((newMaxKeys) <= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("61be3924-5d0e-3bdb-a1cd-d05d3d0051a0"))) ? (((newByteSize & 1) == 0) || ((newMaxKeys) != ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("5f9974ac-51e9-3246-92b1-e75e7541a54a"))) ? ((((newByteSize & 1)) == (0)) && ((1) <= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("fe973350-06b9-3a2c-af07-39eaa842f700"))) ? ((((newByteSize & 1)) == (0)) && ((newMaxKeys) == ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("258805cd-ee31-3886-b305-6d605d02583d"))) ? ((((newByteSize & 1)) == (0)) && ((0) < ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("0892ca21-fc3a-36e3-9c18-da6849410f02"))) ? ((((newByteSize & 1)) == (0)) || ((0) < ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("78407872-9e80-3ea0-a82e-62f556db30a4"))) ? (((newByteSize & 1) == 0) && ((1) >= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("118e4f0e-0873-3da9-9269-234f2ae8a8c2"))) ? (((newByteSize & 1) == 0) || ((0) < ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("e4f99b1c-ba4f-3ad7-8159-17abca8fafb9"))) ? (((newByteSize & 1) == 0) && ((1) == ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("00e3fbfc-70eb-3f6f-99b6-924d47d6699b"))) ? ((((newByteSize & 1)) != (0)) && ((newMaxKeys) > ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("9ec5726e-9111-33f7-a52b-5d845a89fba0"))) ? (((newByteSize & 1) == 0) || ((newMaxKeys) >= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("7cc66247-5f8a-31eb-beb8-f70fe7d65320"))) ? ((((newByteSize & 1)) != (0)) || ((0) <= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("0f0278d7-ff53-34aa-99ec-e32407ad24e1"))) ? ((newMaxKeys) != ((this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("43cd2e0c-cc4f-382f-a471-83db57723c6f"))) ? (((newByteSize & 1) == 0) && (newMaxKeys > (this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("9fbd30cb-a117-3a40-b346-67b0a40d6f62"))) ? ((((newByteSize & 1)) == (0)) || ((newMaxKeys) == ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("476e757a-b5b5-357a-a531-04af43610380"))) ? ((newMaxKeys) == ((this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("3a442a8a-b931-36d9-bb8c-2dbd20245f50"))) ? ((((newByteSize & 1)) == (0)) || ((0) != ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("3455ce92-54bd-3097-9cbb-2637fdaefb0d"))) ? ((1) >= ((this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("da53a312-a0a7-3f36-8722-2db33ae4fbdf"))) ? ((((newByteSize & 1)) != (0)) || ((newMaxKeys) == ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("233aa3a9-0ab5-3d7b-ace9-cd7c7f7d9d9f"))) ? ((((newByteSize & 1)) == (0)) || ((newMaxKeys) > ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("bddb59d6-7e77-3092-9331-cd92ad5a8699"))) ? (((newByteSize & 1) == 0) || ((newMaxKeys) > ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("4dd9eb54-7e2d-3574-9fb1-1bbee33e9e9d"))) ? ((((newByteSize & 1)) == (0)) && ((newMaxKeys) > ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("d9cb1366-4880-3a47-8900-ac779bc98a2f"))) ? ((((newByteSize & 1)) != (0)) || ((0) != ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("dd455007-9866-3d93-a207-dadbb4c85dae"))) ? ((((newByteSize & 1)) != (0)) && ((newMaxKeys) < ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("3db15ab8-aea3-36cc-b8d4-1942d0e6a830"))) ? ((((newByteSize & 1)) != (0)) && ((0) > ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("e102e147-49f4-31f9-8eee-a213b420a2b0"))) ? (((newByteSize & 1) == 0) || ((0) <= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("f9eeda08-5095-3102-bfa9-7895131927d1"))) ? (((newByteSize & 1) == 0) || ((newMaxKeys) < ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("0bd27e49-7b7b-3e12-8fd0-3981d8671708"))) ? (newMaxKeys > (this.keyCount << 1)) : (((KnobRuntime.check(java.util.UUID.fromString("12619c13-aee9-32dd-bb87-03bd965a2402"))) ? (((newByteSize & 1) == 0) || ((0) != ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("9ccecac3-073d-351e-90df-e1bf18c350a2"))) ? ((((newByteSize & 1)) != (0)) && ((0) >= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("8694ec70-bbe5-3967-a550-2339e1537b46"))) ? ((((newByteSize & 1)) != (0)) || ((1) == ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("4feeb46a-2fcc-30e0-a3cf-82c0a2b1ee55"))) ? ((((newByteSize & 1)) != (0)) || ((1) < ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("cc6d3988-968f-3444-9b4d-c924d28a4ddc"))) ? (((newByteSize & 1) == 0) || ((1) != ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("0e47c99a-c001-39e2-97a6-e152cb40f8bc"))) ? ((((newByteSize & 1)) == (0)) || ((1) > ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("37ea2638-97e4-3638-ab71-6ccf7f591441"))) ? (((newByteSize & 1) == 0) || ((newMaxKeys) <= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("7a55dd02-a784-301e-a843-ecc7f1ea94a6"))) ? ((((newByteSize & 1)) != (0)) && ((newMaxKeys) >= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("54f313e6-0fde-3610-9d99-6a6417580473"))) ? (((newByteSize & 1) == 0) && ((1) < ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("f55b7064-404f-342a-b798-6017c0d1e7c5"))) ? ((((newByteSize & 1)) == (0)) && ((1) >= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("870b2d61-8b6a-392f-b067-60041dca83de"))) ? (((newByteSize & 1) == 0) || ((1) <= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("9b5de1f6-fb1c-3d61-918e-187672245468"))) ? ((((newByteSize & 1)) != (0)) || ((newMaxKeys) >= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("b4fcca06-c2c4-3a65-8163-909c45a73658"))) ? ((((newByteSize & 1)) != (0)) && ((0) == ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("059fa87d-31df-381f-aaf4-4175d469f4ad"))) ? (((newByteSize & 1) == 0) && ((newMaxKeys) >= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("f435fb35-451e-3957-899c-365fb8007e63"))) ? ((((newByteSize & 1)) == (0)) || ((1) >= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("154cd225-9e36-352f-954a-5426d2dec4dc"))) ? ((((newByteSize & 1)) != (0)) || ((1) > ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("9c0da75a-147b-3aea-bc9f-fa07c0a0fbbf"))) ? ((((newByteSize & 1)) == (0)) || ((0) > ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("8c8b916b-82f7-32f7-8748-c3030e46f713"))) ? ((((newByteSize & 1)) != (0)) || ((1) <= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("e29ed419-7057-3ac8-8044-dd16d038a029"))) ? (((newByteSize & 1) == 0) || ((1) > ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("b591b245-5ccf-36c2-911d-eb05766353a8"))) ? ((newMaxKeys) > ((this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("339d642b-6a2a-3cbb-97ac-59e6ccd6ee54"))) ? ((((newByteSize & 1)) == (0)) && ((1) == ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("ae0a745f-2c8a-3443-8785-a5f41cca8a9a"))) ? ((((newByteSize & 1)) == (0)) || ((newMaxKeys) >= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("b35d6321-3b4c-3369-8a52-cde62afedad6"))) ? ((((newByteSize & 1)) == (0)) || ((1) < ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("2ba17e92-581f-3dc2-a332-f425f596a111"))) ? ((0) <= ((this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("065ae7a9-b3d8-33eb-aa49-051ac8b7eb30"))) ? ((((newByteSize & 1)) == (0)) && ((1) > ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("075a5af3-a16e-34ac-a2b2-e8528f87101d"))) ? ((((newByteSize & 1)) == (0)) && (newMaxKeys > (this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("510b1812-ad70-38aa-aaa0-59f0dd5a9fcf"))) ? ((((newByteSize & 1)) != (0)) || ((newMaxKeys) != ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("6fd46f31-e568-3301-afe5-6f009ec2307a"))) ? ((((newByteSize & 1)) != (0)) || ((0) == ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("b0dd2598-ab12-3c08-9e09-75e1743033a9"))) ? (((newByteSize & 1) == 0) || ((0) > ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("6e71f309-614d-3c35-906d-778f9447e5a4"))) ? (((newByteSize & 1) == 0) && ((0) > ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("6ace8c2a-4ae0-3701-a0ce-a23651740dff"))) ? ((1) < ((this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("bd576998-b8a1-37d7-b183-59c49f0e9e54"))) ? ((((newByteSize & 1)) == (0)) && ((0) == ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("9c43200f-7800-3509-98c1-f053166809ab"))) ? ((newMaxKeys) < ((this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("bafcb0b3-3afa-38d0-9f9c-fd0049d3861f"))) ? ((((newByteSize & 1)) != (0)) && ((1) <= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("8c881ae4-1ba2-3b16-90ec-8ba133ba311c"))) ? ((((newByteSize & 1)) != (0)) || ((newMaxKeys) > ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("a717c4b8-98c0-3b8d-809f-f16ac9cefd4a"))) ? ((1) != ((this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("1a244156-4039-3bb3-978c-ef1c3d0705a0"))) ? ((0) == ((this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("8947e4a4-e2a1-3782-9581-cb53532ae41b"))) ? ((((newByteSize & 1)) == (0)) && ((newMaxKeys) <= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("4be1d373-45c1-36b8-9e59-97a6a900b8d6"))) ? ((((newByteSize & 1)) == (0)) && ((0) >= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("7d9a7c23-57b8-3f43-91ef-a8c967208d49"))) ? (((newByteSize & 1) == 0) || (newMaxKeys > (this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("8713bd51-6b7a-3de8-aa71-6576060aa7bd"))) ? (((newByteSize & 1) == 0) || ((1) < ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("abc44326-96f0-3676-90e1-a25f3c450847"))) ? ((((newByteSize & 1)) == (0)) && ((1) < ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("caac1fe9-8d52-3b45-800a-8c73a864449a"))) ? ((0) != ((this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("28a1fe3f-4dc1-3ff4-84dc-49c3f1e92429"))) ? (((newByteSize & 1) == 0) && ((0) <= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("8f88a1a6-0918-3d5d-b403-6c2c7aa25423"))) ? ((((newByteSize & 1)) == (0)) || ((newMaxKeys) <= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("c546ddb8-948a-3d2e-85e3-fbdbec307478"))) ? ((((newByteSize & 1)) == (0)) || ((1) <= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("63594f01-4e71-380a-89e8-a5e5f01eba3d"))) ? ((((newByteSize & 1)) == (0)) && ((newMaxKeys) != ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("b63615b4-3167-33e3-b9a4-44ee8a496d10"))) ? (((newByteSize & 1) == 0) && ((1) > ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("1658141f-a204-3f62-912e-ff9907fc9656"))) ? (((newByteSize & 1) == 0) || ((1) >= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("97aa5dbc-748c-338f-84a8-d0396a6dae8b"))) ? ((((newByteSize & 1)) != (0)) && ((0) < ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("d4c94f49-8e94-373d-86ee-89fa6103d05a"))) ? ((((newByteSize & 1)) != (0)) && ((1) >= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("5b5881e9-abfc-3c29-a218-f6ba4fa24c15"))) ? (((newByteSize & 1)) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("24b15666-a9ab-3d60-a0c7-2e1816fc49e8"))) ? ((((newByteSize & 1)) != (0)) && ((0) != ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("9100bd25-2443-3d8d-acb1-70c512ee1fb8"))) ? ((((newByteSize & 1)) != (0)) || ((0) < ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("1a18972b-2066-3c17-a2a9-2753803cbf42"))) ? ((((newByteSize & 1)) == (0)) || ((1) == ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("61f59246-b658-345a-9ee4-7ea04835bd1b"))) ? ((((newByteSize & 1)) == (0)) && ((0) <= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("a0d6b9ff-08e2-3b0c-a26e-71b5964f60a2"))) ? ((((newByteSize & 1)) != (0)) && (newMaxKeys > (this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("72136b9b-f015-3d50-aeaa-69f396e97220"))) ? (((newByteSize & 1) == 0) || ((1) == ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("d12e0c66-7392-3ddc-a989-213f41f12459"))) ? (((newByteSize & 1)) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6877c3a4-145d-34ef-9107-3009c6aee778"))) ? (((newByteSize & 1) == 0) && ((1) != ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("115a5236-74ef-3f82-8281-d4ac4a0a142a"))) ? ((((newByteSize & 1)) != (0)) && ((newMaxKeys) == ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("d365f5cc-1616-3f8c-a791-b39871ec5b98"))) ? ((((newByteSize & 1)) != (0)) && ((1) > ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("6b8eb79c-8187-3b25-907a-0acf271cbeb7"))) ? (((newByteSize & 1) == 0) || ((newMaxKeys) == ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("cc96e905-a242-3112-a28f-d1b7e0a5756b"))) ? ((1) > ((this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("f30f20e8-2613-35ab-a3d3-f6df4f96246b"))) ? (((newByteSize & 1) == 0) && ((newMaxKeys) <= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("f29f4b0e-37ec-3e2e-9e44-6e41e5466c45"))) ? ((((newByteSize & 1)) == (0)) && ((1) != ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("d55e51db-d454-37f3-8b58-38b834b4b933"))) ? (((newByteSize & 1) == 0) || ((0) >= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("afe2b013-1ce6-38dd-9873-b970263f3443"))) ? (((newByteSize & 1) == 0) && ((0) != ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("1cc3d820-1bf3-3db2-9a96-803790b8dbdf"))) ? ((((newByteSize & 1)) != (0)) || (newMaxKeys > (this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("aff354b5-4e75-348b-aca8-ce1b0df6118c"))) ? (((newByteSize & 1) == 0) && ((0) == ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("18ee0b4e-5d37-3f34-882f-c9559cd23a86"))) ? ((((newByteSize & 1)) == (0)) || ((1) != ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("5ac4aff9-2eb1-36a9-a877-cb4d5e1eaf2a"))) ? ((((newByteSize & 1)) != (0)) && ((1) == ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("e0c8d62b-8d56-317a-9b14-fa45824a08da"))) ? ((((newByteSize & 1)) == (0)) && ((newMaxKeys) < ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("e9b7c2c7-93d6-3062-8c01-f943391d8674"))) ? ((((newByteSize & 1)) != (0)) || ((0) > ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("2d12ee7f-e6d6-3509-b696-e2d8cf238e5e"))) ? ((((newByteSize & 1)) == (0)) || ((newMaxKeys) < ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("3f0b8f34-97ab-38dc-ae58-966e39d7dcf9"))) ? (((newByteSize & 1) == 0) && ((1) <= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("bb3217e0-b311-33b1-ac28-ada103f518b5"))) ? ((((newByteSize & 1)) == (0)) || ((0) <= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("8df60aa7-2e4d-386f-bd7f-c86ab6ff645b"))) ? ((((newByteSize & 1)) != (0)) || ((0) >= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("d3b31aec-05a4-3c77-85d2-b85c09d62eb4"))) ? ((((newByteSize & 1)) == (0)) || ((0) == ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("cc7712ec-7b92-3f28-83d1-f950f484f54f"))) ? ((((newByteSize & 1)) != (0)) && ((newMaxKeys) <= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("9bbf036a-eb2f-34b5-a6ac-8099e77f9b6d"))) ? ((((newByteSize & 1)) != (0)) && ((newMaxKeys) != ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("4b2b59c4-64f8-355b-aca0-a0e8663dcf88"))) ? ((0) < ((this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("b3d49846-c2b6-321a-b021-999141b02bc4"))) ? ((((newByteSize & 1)) == (0)) && ((newMaxKeys) >= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("a191faba-26a7-381b-8907-1c9891304f50"))) ? ((((newByteSize & 1)) != (0)) && ((0) <= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("3e41b30e-fc6d-3e2c-b7c5-1f30e5464974"))) ? ((((newByteSize & 1)) != (0)) || ((1) >= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("4d223015-e5d4-3cd3-b4f9-454c4efcad93"))) ? ((((newByteSize & 1)) != (0)) && ((1) < ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("cbaacc11-171f-3f68-81fe-0db8e0d9b501"))) ? ((0) > ((this.keyCount << 1))) : (((KnobRuntime.check(java.util.UUID.fromString("2444edd8-85fc-3e1b-8c5a-ce25062f0fbf"))) ? ((((newByteSize & 1)) != (0)) && ((1) != ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("70c29aef-4186-36ba-b05b-09de090d448b"))) ? (((newByteSize & 1) == 0) && ((newMaxKeys) != ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("74460db2-4414-3414-8b40-fc29ff4ffdc7"))) ? ((((newByteSize & 1)) == (0)) || ((0) >= ((this.keyCount << 1)))) : (((KnobRuntime.check(java.util.UUID.fromString("a42eb61c-5214-36a8-8f6d-1abc6f21497d"))) ? (((newByteSize & 1) == 0) || ((0) == ((this.keyCount << 1)))) : ((newByteSize & 1) == 0 && newMaxKeys > (this.keyCount << 1)))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
        pieces <<= 1;
        newByteSize >>= 1;
        newMaxKeys >>= 1;
      }

      // if we should fold these into pieces
      if (((KnobRuntime.check(java.util.UUID.fromString("7aef8685-d005-3d8e-95cf-53d5d384f912"))) ? ((pieces) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("11b9901c-4e17-3bf7-9870-b2115fa67437"))) ? ((pieces) >= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("c90d9165-a8fd-31df-ba68-eb12e290ca16"))) ? ((pieces) < (1)) : (((KnobRuntime.check(java.util.UUID.fromString("1d0f6cb6-d4b2-320c-9137-5aa1ed4aba0e"))) ? ((pieces) > (1)) : (((KnobRuntime.check(java.util.UUID.fromString("8717cb03-5586-3dc0-b475-54723559dc0d"))) ? ((pieces) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("0b8206b0-49a2-375b-9ee1-6aa6115f6ca2"))) ? ((pieces) <= (1)) : (pieces > 1))))))))))))) {
        byte[] array = this.bloom.array();
        int start = this.bloom.arrayOffset();
        int end = start + newByteSize;
        int off = end;
        for (int p = 1; p < pieces; ++p) {
          for (int pos = start; pos < end; ++pos) {
            array[pos] |= array[off++];
          }
        }
        // folding done, only use a subset of this array
        if (KnobRuntime.check(java.util.UUID.fromString("30aaaaaf-6b89-3b96-98e7-3481d5d35c74"))) { this.bloom.slice(); } else { this.bloom.rewind(); }
if(KnobRuntime.check(java.util.UUID.fromString("6e322955-dfc8-395c-98fe-bddc028fb12d"))) {
newByteSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d795fa68-30d9-302d-b3ae-bfffc31102b9"))) {
newByteSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("e3f9a3a4-61d2-3312-8937-ccd5036c5cee"))) {
newByteSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("61f12976-353b-391b-8ff2-b9c09f75ad20"))) {
newByteSize *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("088d0f2c-1bff-34c3-aaa8-864f38ddc77e"))) {
newByteSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("d1f5c48f-86c0-373d-a50f-1b74e13d6e5f"))) {
newByteSize /= 2;
}
        if (KnobRuntime.check(java.util.UUID.fromString("41159560-e4e0-397a-977f-e4c4000fde08"))) { ByteBuffer.allocate((newByteSize & 1)); } else if (KnobRuntime.check(java.util.UUID.fromString("2cdfe202-d043-313c-8cdc-88c6a6c1ae13"))) { this.bloom.limit(newByteSize & 1); } else if (KnobRuntime.check(java.util.UUID.fromString("6dc51852-f142-31a2-a652-fa7efdf62d31"))) { ByteBuffer.allocate(1); } else if (KnobRuntime.check(java.util.UUID.fromString("721cd151-f7ee-3e57-a070-35922832737c"))) { ByteBuffer.allocate(newByteSize & 1); } else if (KnobRuntime.check(java.util.UUID.fromString("6be1457f-8cfc-3ad7-a255-799052ff78f8"))) { this.bloom.limit(1); } else if (KnobRuntime.check(java.util.UUID.fromString("e3eda13e-7719-3e9b-b93f-5b8cd012f1ce"))) { this.bloom.limit(0); } else if (KnobRuntime.check(java.util.UUID.fromString("56517ec5-8115-39bd-a993-52f8dc48acda"))) { this.bloom.limit((newByteSize & 1)); } else if (KnobRuntime.check(java.util.UUID.fromString("2dc53ea9-da86-30e1-87e8-82a984d7f522"))) { ByteBuffer.allocate(0); } else { this.bloom.limit(newByteSize); }
        this.bloom = this.bloom.slice();
        this.byteSize = newByteSize;
        this.maxKeys = newMaxKeys;
      }
    }
  }

  /**
   * Writes just the bloom filter to the output array
   * @param out OutputStream to place bloom
   * @throws IOException Error writing bloom array
   */
  public void writeBloom(final DataOutput out) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("f7c0ef71-6d88-3827-b36a-06a817e3eeda"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("51805915-3461-377e-a1d5-776ecca077e7"))) {
throw new java.io.IOException("Injected exception");
}
    if (!this.bloom.hasArray()) {
      throw new IOException("Only writes ByteBuffer with underlying array.");
    }
if(KnobRuntime.check(java.util.UUID.fromString("4f48c0c8-dce4-3523-a3cb-b81b99ab1d61"))) {
throw new java.io.IOException("Injected exception");
}
    if (KnobRuntime.check(java.util.UUID.fromString("e5e98ca5-e775-38fd-8f9f-722f32717c4e"))) { out.write(this.bloom.array(), this.bloom.limit(), this.bloom.limit()); } else if (KnobRuntime.check(java.util.UUID.fromString("19e805aa-db3b-3369-8fcf-fafc5138d9ea"))) { out.write(this.bloom.array(), this.bloom.arrayOffset(), this.bloom.arrayOffset()); } else { out.write(this.bloom.array(), this.bloom.arrayOffset(), this.bloom.limit()); }
  }

  public int getHashCount() {
if(KnobRuntime.check(java.util.UUID.fromString("2d6e82f5-f19a-344c-8bd6-f77291a772fe"))) {
return 0;
}
    return hashCount;
  }

  @Override
  public String toString() {
    return BloomFilterUtil.toString(this);
  }

}
