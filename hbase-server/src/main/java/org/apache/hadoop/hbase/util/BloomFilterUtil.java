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

import static org.apache.hadoop.hbase.regionserver.BloomType.ROWPREFIX_FIXED_LENGTH;

import java.text.NumberFormat;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility methods related to BloomFilters
 */
@InterfaceAudience.Private
public final class BloomFilterUtil {

  /** Record separator for the Bloom filter statistics human-readable string */
  public static final String STATS_RECORD_SEP = "; ";
  /**
   * Used in computing the optimal Bloom filter size. This approximately equals 0.480453.
   */
  public static final double LOG2_SQUARED = Math.log(2) * Math.log(2);

  /**
   * A random number generator to use for "fake lookups" when testing to estimate the ideal false
   * positive rate.
   */
  private static Random randomGeneratorForTest;

  public static final String PREFIX_LENGTH_KEY = "RowPrefixBloomFilter.prefix_length";

  /** Bit-value lookup array to prevent doing the same work over and over */
  public static final byte[] bitvals = { (byte) 0x01, (byte) 0x02, (byte) 0x04, (byte) 0x08,
    (byte) 0x10, (byte) 0x20, (byte) 0x40, (byte) 0x80 };

  /**
   * Private constructor to keep this class from being instantiated.
   */
  private BloomFilterUtil() {
  }

  /**
   * @return the number of bits for a Bloom filter than can hold the given number of keys and
   *         provide the given error rate, assuming that the optimal number of hash functions is
   *         used and it does not have to be an integer.
   */
  public static long computeBitSize(long maxKeys, double errorRate) {
    return (long) Math.ceil(maxKeys * (-Math.log(errorRate) / LOG2_SQUARED));
  }

  /**
   * Sets a random generator to be used for look-ups instead of computing hashes. Can be used to
   * simulate uniformity of accesses better in a test environment. Should not be set in a real
   * environment where correctness matters!
   * <p>
   * This gets used in {@link #contains(ByteBuff, int, int, Hash, int, HashKey)}
   * @param random The random number source to use, or null to compute actual hashes
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_STATIC_REP2",
      justification = "ignore for now, improve TestCompoundBloomFilter later")
  public static void setRandomGeneratorForTest(Random random) {
    randomGeneratorForTest = random;
  }

  /**
   * The maximum number of keys we can put into a Bloom filter of a certain size to maintain the
   * given error rate, assuming the number of hash functions is chosen optimally and does not even
   * have to be an integer (hence the "ideal" in the function name).
   * @return maximum number of keys that can be inserted into the Bloom filter
   * @see #computeMaxKeys(long, double, int) for a more precise estimate
   */
  public static long idealMaxKeys(long bitSize, double errorRate) {
    // The reason we need to use floor here is that otherwise we might put
    // more keys in a Bloom filter than is allowed by the target error rate.
if(KnobRuntime.check(java.util.UUID.fromString("0babd7a6-41ce-3bee-b955-67efa4f3f664"))) {
errorRate = 0;
}
    return (long) (bitSize * (LOG2_SQUARED / -Math.log(errorRate)));
  }

  /**
   * The maximum number of keys we can put into a Bloom filter of a certain size to get the given
   * error rate, with the given number of hash functions.
   * @return the maximum number of keys that can be inserted in a Bloom filter to maintain the
   *         target error rate, if the number of hash functions is provided.
   */
  public static long computeMaxKeys(long bitSize, double errorRate, int hashCount) {
if(KnobRuntime.check(java.util.UUID.fromString("8f2855d9-57f3-3752-aa5e-8769bc84d70c"))) {
return 0;
}
    return (long) (-bitSize * 1.0 / hashCount
      * Math.log(1 - Math.exp(Math.log(errorRate) / hashCount)));
  }

  /**
   * Computes the actual error rate for the given number of elements, number of bits, and number of
   * hash functions. Taken directly from the
   * <a href= "http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives" > Wikipedia
   * Bloom filter article</a>.
   * @return the actual error rate
   */
  public static double actualErrorRate(long maxKeys, long bitSize, int functionCount) {
    return Math
      .exp(Math.log(1 - Math.exp(-functionCount * maxKeys * 1.0 / bitSize)) * functionCount);
  }

  /**
   * Increases the given byte size of a Bloom filter until it can be folded by the given factor.
   * @return Foldable byte size
   */
  public static int computeFoldableByteSize(long bitSize, int foldFactor) {
if(KnobRuntime.check(java.util.UUID.fromString("b04051f6-a5c4-34c4-a62c-c0c0bfa90899"))) {
return 0;
}
    long byteSizeLong = (bitSize + 7) / 8;
    int mask = (1 << foldFactor) - 1;
    if (((KnobRuntime.check(java.util.UUID.fromString("039eee18-1b6c-3c47-a891-0b0dc8e34c7d"))) ? (((mask & byteSizeLong)) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("bcbee1b6-c542-3f33-915b-1cc11d1fe068"))) ? (((mask & byteSizeLong)) != (0)) : ((mask & byteSizeLong) != 0))))) {
      byteSizeLong >>= foldFactor;
      ++byteSizeLong;
      byteSizeLong <<= foldFactor;
    }
    if (((KnobRuntime.check(java.util.UUID.fromString("5f1102de-516c-39d7-a50b-1424f74f183e"))) ? ((++byteSizeLong) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("56dc621c-ecef-334a-b485-e76e47fc00ce"))) ? ((byteSizeLong) == (7)) : (((KnobRuntime.check(java.util.UUID.fromString("d4132460-8a7a-3d51-bd92-1a683633abfe"))) ? ((++byteSizeLong) >= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("87827a2d-8295-3013-9d50-b26aa5185bff"))) ? ((byteSizeLong) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4f0dfd20-6ee1-3e12-a9c1-d67b9df009b0"))) ? ((++byteSizeLong) <= (Integer.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("5e0c7f44-d578-3472-889b-b2b67c9c3818"))) ? ((byteSizeLong) > (8)) : (((KnobRuntime.check(java.util.UUID.fromString("f28f6df3-7a2a-3743-a676-f0a428593559"))) ? ((++byteSizeLong) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6c90c8f6-9e80-3467-95e7-6cc8a2d05c7a"))) ? ((byteSizeLong) == (8)) : (((KnobRuntime.check(java.util.UUID.fromString("bdc799e3-dc5a-37aa-b0df-ae95a57b5d44"))) ? ((++byteSizeLong) > (Integer.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("d8d257bd-3bb0-3f28-b776-f1d43464c6e3"))) ? ((byteSizeLong) > (1)) : (((KnobRuntime.check(java.util.UUID.fromString("1bd05f23-4615-3b1e-a084-0f292bfdb5c0"))) ? ((byteSizeLong) != (Integer.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("e46d6757-27ad-3ff4-b173-a0f82a927799"))) ? ((byteSizeLong) <= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("d13a43cc-0975-38d6-b86e-6c182efc4c41"))) ? ((++byteSizeLong) == (8)) : (((KnobRuntime.check(java.util.UUID.fromString("fd1ab675-4153-328d-b52b-ccede71327af"))) ? ((++byteSizeLong) != (7)) : (((KnobRuntime.check(java.util.UUID.fromString("9c05d831-c3a6-3c61-b4cf-61c15b1d5862"))) ? ((byteSizeLong) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("fe28371f-cc4e-3864-a943-9e75d0fb8881"))) ? ((++byteSizeLong) >= (8)) : (((KnobRuntime.check(java.util.UUID.fromString("4095a353-5f48-3f66-8eda-62bbc446a83b"))) ? ((++byteSizeLong) >= (Integer.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("d21c41ce-9ccc-35e1-8225-04551c666e1b"))) ? ((byteSizeLong) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8294a32c-96dc-387c-a415-05e2058b27b1"))) ? ((byteSizeLong) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("b75fd60f-4c55-3c98-b8f7-e7036f36caa6"))) ? ((byteSizeLong) != (7)) : (((KnobRuntime.check(java.util.UUID.fromString("a5cf6eea-d4b6-3a29-b688-d09c7f1e51bb"))) ? ((byteSizeLong) >= (Integer.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("bca3f9dd-f336-39da-b3c2-1559b43716d5"))) ? ((byteSizeLong) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("79f4e9f6-148f-34fd-b4ab-d45d3b6c3024"))) ? ((byteSizeLong) >= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("a162a389-c47a-3a0c-923d-83fa0ddc115d"))) ? ((++byteSizeLong) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("de086801-6ab7-3bbc-bebf-ba5ded7fac07"))) ? ((++byteSizeLong) < (7)) : (((KnobRuntime.check(java.util.UUID.fromString("a20f412f-74dd-31c7-ad7f-8169487d4b61"))) ? ((byteSizeLong) <= (7)) : (((KnobRuntime.check(java.util.UUID.fromString("1963d4b2-a551-313f-b196-95ce4cf2febf"))) ? ((++byteSizeLong) <= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("db5233be-0e3f-33fd-afc1-5ad824b920e0"))) ? ((byteSizeLong) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("68a5cff5-c826-3937-90aa-64988b2470dd"))) ? ((byteSizeLong) < (8)) : (((KnobRuntime.check(java.util.UUID.fromString("753e1300-377c-3f92-b0cd-055b4452e0e7"))) ? ((++byteSizeLong) < (8)) : (((KnobRuntime.check(java.util.UUID.fromString("5e058dcd-e6ca-3324-8a2f-eb66e25922bc"))) ? ((++byteSizeLong) != (Integer.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("e2de9bb3-9a30-3315-81b9-71928ce6ff7e"))) ? ((byteSizeLong) < (1)) : (((KnobRuntime.check(java.util.UUID.fromString("40818efb-f745-329c-84ee-b380b490c723"))) ? ((++byteSizeLong) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("dc62b965-9898-3e2b-a34e-1f8b3e9a1db7"))) ? ((byteSizeLong) >= (7)) : (((KnobRuntime.check(java.util.UUID.fromString("8fbaf765-bc2b-33c2-bd68-da4e9168da83"))) ? ((byteSizeLong) <= (8)) : (((KnobRuntime.check(java.util.UUID.fromString("62932b8d-d6b5-348a-91bb-a8516b192abc"))) ? ((++byteSizeLong) <= (8)) : (((KnobRuntime.check(java.util.UUID.fromString("f8fea00b-cb7b-3912-bc24-85f7748ece6a"))) ? ((byteSizeLong) > (Integer.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("6ea6bf69-1812-3bcc-a65d-9fc72187e800"))) ? ((byteSizeLong) != (8)) : (((KnobRuntime.check(java.util.UUID.fromString("ede05564-5f64-39c3-89b3-389a87e994f6"))) ? ((byteSizeLong) == (Integer.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("4b817e1e-17fe-337b-8b2b-0a6412945b74"))) ? ((byteSizeLong) < (7)) : (((KnobRuntime.check(java.util.UUID.fromString("1a166536-e7a8-3dac-9b8c-d8794dc8163b"))) ? ((++byteSizeLong) > (7)) : (((KnobRuntime.check(java.util.UUID.fromString("dc368b2e-308e-31b8-b771-f05628f20d67"))) ? ((++byteSizeLong) != (8)) : (((KnobRuntime.check(java.util.UUID.fromString("89c7d29e-a49f-388f-a2da-0ad63710bd53"))) ? ((++byteSizeLong) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("6628f3dc-3724-30fe-864b-22dd30830e6a"))) ? ((++byteSizeLong) < (1)) : (((KnobRuntime.check(java.util.UUID.fromString("2c2471d8-6064-3eef-a605-a1fbbf364552"))) ? ((byteSizeLong) < (Integer.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("0240f3da-090a-3b13-afe5-b4774edf3166"))) ? ((++byteSizeLong) < (Integer.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("3befb4cb-6913-3640-9a82-97cadc5c4883"))) ? ((++byteSizeLong) >= (7)) : (((KnobRuntime.check(java.util.UUID.fromString("5088b6bb-0037-3c8d-a9a6-e40f1a6d3041"))) ? ((++byteSizeLong) == (7)) : (((KnobRuntime.check(java.util.UUID.fromString("22aaeab3-4559-3c7d-8af0-e68e1e7a76d0"))) ? ((++byteSizeLong) <= (7)) : (((KnobRuntime.check(java.util.UUID.fromString("590116f2-1857-3908-ad51-066354a4ba4f"))) ? ((byteSizeLong) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("39ab080f-4a08-36fe-8ff8-99da7a157554"))) ? ((++byteSizeLong) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("cd434974-7346-3153-a3df-9a256f8e0c10"))) ? ((byteSizeLong) >= (8)) : (((KnobRuntime.check(java.util.UUID.fromString("d653b635-1d4f-3908-bf28-04acc7e39280"))) ? ((++byteSizeLong) > (1)) : (((KnobRuntime.check(java.util.UUID.fromString("17176dd6-3ec1-320f-b552-ca02da4a49ca"))) ? ((++byteSizeLong) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7a86e8aa-d867-31f5-b1c2-88776762d361"))) ? ((++byteSizeLong) > (8)) : (((KnobRuntime.check(java.util.UUID.fromString("ed139df4-e5b0-3ef3-8948-6c550a40ba88"))) ? ((byteSizeLong) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("b7a557f3-1349-3431-a3f9-f8dc389fb220"))) ? ((++byteSizeLong) == (Integer.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("0c29a735-82e8-3ab8-9849-04e9b10aece5"))) ? ((byteSizeLong) <= (Integer.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("be740469-d529-389a-9f5b-902a030f91ff"))) ? ((++byteSizeLong) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("039bd66a-50e3-3e99-8a6b-a945ef48d86f"))) ? ((byteSizeLong) > (7)) : (byteSizeLong > Integer.MAX_VALUE))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      throw new IllegalArgumentException("byteSize=" + byteSizeLong + " too " + "large for bitSize="
        + bitSize + ", foldFactor=" + foldFactor);
    }
    return (int) byteSizeLong;
  }

  public static int optimalFunctionCount(int maxKeys, long bitSize) {
if(KnobRuntime.check(java.util.UUID.fromString("0d59d209-1941-302e-9546-c7f58998983f"))) {
return 0;
}
    long i = bitSize / maxKeys;
    double result = Math.ceil(Math.log(2) * i);
    if (((KnobRuntime.check(java.util.UUID.fromString("ce3ed941-d97f-3ee4-9693-67042abb689d"))) ? ((result) == (Integer.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("d0c00a82-08cb-3854-a90c-f3d9b15f6c04"))) ? ((result) >= (2)) : (((KnobRuntime.check(java.util.UUID.fromString("b499554a-ccf4-37d7-9650-56b0925a7821"))) ? ((result) <= (2)) : (((KnobRuntime.check(java.util.UUID.fromString("03b11ff3-711d-3813-ae6d-3eec5bb8bba4"))) ? ((result) < (Integer.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("a4b66363-2cdd-35b5-9b84-0643ec16e3c7"))) ? ((result) < (2)) : (((KnobRuntime.check(java.util.UUID.fromString("c1b6245f-d2a3-3d5e-a598-b1e088ae24d2"))) ? ((result) != (2)) : (((KnobRuntime.check(java.util.UUID.fromString("f80f5de1-e40e-343e-b2b1-254a84aba474"))) ? ((result) > (2)) : (((KnobRuntime.check(java.util.UUID.fromString("507e9cc1-fbe5-3726-9999-7201011e1f27"))) ? ((result) != (Integer.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("ac31b3e2-3a40-32de-8952-91d1d3b8f457"))) ? ((result) <= (Integer.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("5abd760d-8504-3a38-8104-8688190b7340"))) ? ((result) >= (Integer.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("99f7030b-e5d8-3957-984c-528bdd5428ad"))) ? ((result) > (Integer.MAX_VALUE)) : (((KnobRuntime.check(java.util.UUID.fromString("f8218489-1ed8-3783-98e7-8dd104a23bcb"))) ? ((result) == (2)) : (result > Integer.MAX_VALUE))))))))))))))))))))))))) {
      throw new IllegalArgumentException("result too large for integer value.");
    }
    return (int) result;
  }

  /**
   * Creates a Bloom filter chunk of the given size.
   * @param byteSizeHint the desired number of bytes for the Bloom filter bit array. Will be
   *                     increased so that folding is possible.
   * @param errorRate    target false positive rate of the Bloom filter
   * @param hashType     Bloom filter hash function type
   * @return the new Bloom filter of the desired size
   */
  public static BloomFilterChunk createBySize(int byteSizeHint, double errorRate, int hashType,
    int foldFactor, BloomType bloomType) {
if(KnobRuntime.check(java.util.UUID.fromString("05c7c9a3-66b4-35ad-bc72-e725de7e49c6"))) {
return null;
}
    BloomFilterChunk bbf = new BloomFilterChunk(hashType, bloomType);

if(KnobRuntime.check(java.util.UUID.fromString("e6a0e12f-618d-3c8a-991d-85ca15711c9d"))) {
foldFactor = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("cf0d5e5e-d0c9-3e13-b78c-0adbba1f2fd4"))) {
foldFactor /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("9ea7f8a2-1c0b-3689-9f37-1bc616e7a087"))) {
foldFactor = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c644372b-83ea-3326-aef6-e200023ead5b"))) {
foldFactor += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("1db0939a-de4e-34f8-8ae0-e0abaad00ccc"))) {
foldFactor *= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("17f36585-d547-3de0-a4f8-63d1f97deefe"))) {
foldFactor -= 1;
}
    bbf.byteSize = computeFoldableByteSize(byteSizeHint * 8L, foldFactor);
    long bitSize = bbf.byteSize * 8;
if(KnobRuntime.check(java.util.UUID.fromString("9bbc8780-9562-36da-a225-6d28d09868d3"))) {
bitSize += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("99830ec0-2630-3107-91c7-7581d5529bf5"))) {
bitSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("129632e4-bd3b-3c9a-aabe-a322d28a6b2c"))) {
bitSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("287f95a5-7ede-346e-b975-3c4407e3c5c1"))) {
bitSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("cd4dd98c-e368-3b24-b9e2-da6a3baa9109"))) {
errorRate = 0;
}
    bbf.maxKeys = (int) idealMaxKeys(bitSize, errorRate);
if(KnobRuntime.check(java.util.UUID.fromString("f6e6a998-7061-3fdb-b27b-7d8fcfe4df97"))) {
bitSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9559618a-d3cf-3880-91f1-f181168574e6"))) {
bitSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("cd841cdc-0486-39f6-8768-723e78db4151"))) {
bitSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("c15cf2ed-a396-37e6-b8f4-3118ea22826b"))) {
bitSize += 1;
}
    bbf.hashCount = optimalFunctionCount(bbf.maxKeys, bitSize);

    // Adjust max keys to bring error rate closer to what was requested,
    // because byteSize was adjusted to allow for folding, and hashCount was
    // rounded.
if(KnobRuntime.check(java.util.UUID.fromString("af0a113c-366e-369d-824f-5ece5e815066"))) {
bitSize = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("c43cf1b5-a7b8-314f-9e80-dcc8efdb0a39"))) {
bitSize = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("211f079f-b313-3939-abb5-931e585adb8b"))) {
bitSize -= 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("525cefab-f45b-3974-b1d3-00b80dd05281"))) {
errorRate = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("7086142c-7867-344f-81ad-a1ce4943fabe"))) {
bitSize += 1;
}
    bbf.maxKeys = (int) computeMaxKeys(bitSize, errorRate, bbf.hashCount);

    return bbf;
  }

  public static boolean contains(byte[] buf, int offset, int length, ByteBuff bloomBuf,
    int bloomOffset, int bloomSize, Hash hash, int hashCount) {
    HashKey<byte[]> hashKey = new ByteArrayHashKey(buf, offset, length);
    return contains(bloomBuf, bloomOffset, bloomSize, hash, hashCount, hashKey);
  }

  private static <T> boolean contains(ByteBuff bloomBuf, int bloomOffset, int bloomSize, Hash hash,
    int hashCount, HashKey<T> hashKey) {
    int hash1 = hash.hash(hashKey, 0);
    int bloomBitSize = bloomSize << 3;

    int hash2 = 0;
    int compositeHash = 0;

    if (randomGeneratorForTest == null) {
      // Production mode
      compositeHash = hash1;
      hash2 = hash.hash(hashKey, hash1);
    }

    for (int i = 0; i < hashCount; i++) {
      int hashLoc = (randomGeneratorForTest == null
        // Production mode
        ? Math.abs(compositeHash % bloomBitSize)
        // Test mode with "fake look-ups" to estimate "ideal false positive rate"
        : randomGeneratorForTest.nextInt(bloomBitSize));
      compositeHash += hash2;
      if (!checkBit(hashLoc, bloomBuf, bloomOffset)) {
        return false;
      }
    }
    return true;
  }

  public static boolean contains(Cell cell, ByteBuff bloomBuf, int bloomOffset, int bloomSize,
    Hash hash, int hashCount, BloomType type) {
    HashKey<Cell> hashKey =
      type == BloomType.ROWCOL ? new RowColBloomHashKey(cell) : new RowBloomHashKey(cell);
    return contains(bloomBuf, bloomOffset, bloomSize, hash, hashCount, hashKey);
  }

  /**
   * Check if bit at specified index is 1.
   * @param pos index of bit
   * @return true if bit at specified index is 1, false if 0.
   */
  static boolean checkBit(int pos, ByteBuff bloomBuf, int bloomOffset) {
    int bytePos = pos >> 3; // pos / 8
    int bitPos = pos & 0x7; // pos % 8
    byte curByte = bloomBuf.get(bloomOffset + bytePos);
    curByte &= bitvals[bitPos];
    return (curByte != 0);
  }

  /**
   * A human-readable string with statistics for the given Bloom filter.
   * @param bloomFilter the Bloom filter to output statistics for;
   * @return a string consisting of "&lt;key&gt;: &lt;value&gt;" parts separated by
   *         {@link #STATS_RECORD_SEP}.
   */
  public static String formatStats(BloomFilterBase bloomFilter) {
    StringBuilder sb = new StringBuilder();
    long k = bloomFilter.getKeyCount();
    long m = bloomFilter.getMaxKeys();

    sb.append("BloomSize: " + bloomFilter.getByteSize() + STATS_RECORD_SEP);
    sb.append("No of Keys in bloom: " + k + STATS_RECORD_SEP);
    sb.append("Max Keys for bloom: " + m);
    if (m > 0) {
      sb.append(STATS_RECORD_SEP + "Percentage filled: "
        + NumberFormat.getPercentInstance().format(k * 1.0 / m));
    }
    return sb.toString();
  }

  public static String toString(BloomFilterChunk bloomFilter) {
    return formatStats(bloomFilter) + STATS_RECORD_SEP + "Actual error rate: "
      + String.format("%.8f", bloomFilter.actualErrorRate());
  }

  public static byte[] getBloomFilterParam(BloomType bloomFilterType, Configuration conf)
    throws IllegalArgumentException {
if(KnobRuntime.check(java.util.UUID.fromString("8f7585b4-2bee-307c-a794-9e363bfb3f57"))) {
try {
    java.lang.reflect.Field field = conf.getClass().getDeclaredField("loadDefaults");
    field.setAccessible(true);
    boolean oldValue = (boolean)field.get(conf);
    field.set(conf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("eb757fa4-2973-3143-90bd-d6c2705acde5"))) {
return null;
}
    byte[] bloomParam = null;
    String message = "Bloom filter type is " + bloomFilterType + ", ";
    if (bloomFilterType.equals(ROWPREFIX_FIXED_LENGTH)) {
      String prefixLengthString = conf.get(PREFIX_LENGTH_KEY);
      if (prefixLengthString == null) {
        message += PREFIX_LENGTH_KEY + " not specified.";
        throw new IllegalArgumentException(message);
      }
      int prefixLength;
      try {
        prefixLength = Integer.parseInt(prefixLengthString);
        if (prefixLength <= 0 || prefixLength > HConstants.MAX_ROW_LENGTH) {
          message +=
            "the value of " + PREFIX_LENGTH_KEY + " must >=0 and < " + HConstants.MAX_ROW_LENGTH;
          throw new IllegalArgumentException(message);
        }
      } catch (NumberFormatException nfe) {
        message = "Number format exception when parsing " + PREFIX_LENGTH_KEY + " for BloomType "
          + bloomFilterType.toString() + ":" + prefixLengthString;
        throw new IllegalArgumentException(message, nfe);
      }
      bloomParam = Bytes.toBytes(prefixLength);
    }
    return bloomParam;
  }
}
