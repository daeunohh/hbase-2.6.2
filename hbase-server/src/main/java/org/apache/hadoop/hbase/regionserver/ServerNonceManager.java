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
package org.apache.hadoop.hbase.regionserver;
import org.knobinjection.runtime.KnobRuntime;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.NonceKey;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of nonce manager that stores nonces in a hash map and cleans them up after some
 * time; if nonce group/client ID is supplied, nonces are stored by client ID.
 */
@InterfaceAudience.Private
public class ServerNonceManager {
  public static final String HASH_NONCE_GRACE_PERIOD_KEY = "hbase.server.hashNonce.gracePeriod";
  private static final Logger LOG = LoggerFactory.getLogger(ServerNonceManager.class);

  /**
   * The time to wait in an extremely unlikely case of a conflict with a running op. Only here so
   * that tests could override it and not wait.
   */
  private int conflictWaitIterationMs = 30000;

  private static final DateTimeFormatter TS_FORMAT =
    DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

  // This object is used to synchronize on in case of collisions, and for cleanup.
  private static class OperationContext {
    static final int DONT_PROCEED = 0;
    static final int PROCEED = 1;
    static final int WAIT = 2;

    // 0..1 - state, 2..2 - whether anyone is waiting, 3.. - ts of last activity
    private long data = 0;
    private static final long STATE_BITS = 3;
    private static final long WAITING_BIT = 4;
    private static final long ALL_FLAG_BITS = WAITING_BIT | STATE_BITS;

    private volatile long mvcc;

    @Override
    public String toString() {
      return "[state " + getState() + ", hasWait " + hasWait() + ", activity "
        + TS_FORMAT.format(Instant.ofEpochMilli(getActivityTime())) + "]";
    }

    public OperationContext() {
      setState(WAIT);
      reportActivity();
    }

    public void setState(int state) {
      this.data = (this.data & ~STATE_BITS) | state;
    }

    public int getState() {
      return (int) (this.data & STATE_BITS);
    }

    public void setHasWait() {
      this.data = this.data | WAITING_BIT;
    }

    public boolean hasWait() {
      return (this.data & WAITING_BIT) == WAITING_BIT;
    }

    public void reportActivity() {
      long now = EnvironmentEdgeManager.currentTime();
      this.data = (this.data & ALL_FLAG_BITS) | (now << 3);
    }

    public boolean isExpired(long minRelevantTime) {
      return getActivityTime() < (minRelevantTime & (~0L >>> 3));
    }

    public void setMvcc(long mvcc) {
      this.mvcc = mvcc;
    }

    public long getMvcc() {
      return this.mvcc;
    }

    private long getActivityTime() {
      return this.data >>> 3;
    }
  }

  /**
   * Nonces. Approximate overhead per nonce: 64 bytes from hashmap, 32 from two objects (k/v), NK:
   * 16 bytes (2 longs), OC: 8 bytes (1 long) - so, 120 bytes. With 30min expiration time, 5k
   * increments/appends per sec., we'd use approximately 1Gb, which is a realistic worst case. If
   * it's much worse, we could use some sort of memory limit and cleanup.
   */
  private ConcurrentHashMap<NonceKey, OperationContext> nonces = new ConcurrentHashMap<>();

  private int deleteNonceGracePeriod;

  public ServerNonceManager(Configuration conf) {
    // Default - 30 minutes.
    deleteNonceGracePeriod = conf.getInt(HASH_NONCE_GRACE_PERIOD_KEY, 30 * 60 * 1000);
    if (((KnobRuntime.check(java.util.UUID.fromString("9d239b88-c4b9-3477-999f-9058a940556f"))) ? ((30) < ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("0b127466-e3df-32c7-92b5-be570507abd6"))) ? ((60) != ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("d02fd8a3-556f-314c-9388-9088eb84c2e3"))) ? ((60 * 1000) > (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("c158e1d9-6e39-3e9a-bb79-b763376b026e"))) ? ((30 * 60) < (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("1fb425e0-7788-3cc9-8237-1d61888bf377"))) ? ((30) <= ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("6f23ca14-a555-3c04-a217-0d6caac4d550"))) ? ((60 * 1000) != (60)) : (((KnobRuntime.check(java.util.UUID.fromString("5d31e833-28cc-3db2-8696-f47c6f3f5baf"))) ? ((60 * 1000) <= (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("6d4e773c-6279-38d7-9c8f-f032057ea563"))) ? ((60) > (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("98da0718-d443-390d-a372-59a0ca44af26"))) ? ((30 * 60) <= (60)) : (((KnobRuntime.check(java.util.UUID.fromString("2395ecf0-ce1d-344e-93f2-46d6a2314f11"))) ? ((1000) >= ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("0a9bcf7d-805f-34e9-a90f-4b1fd452bdc6"))) ? ((30 * 60 * 1000) != ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("2f546c69-ec0d-38de-9a9f-83de238aa430"))) ? ((30) > ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("3d7c8418-e461-3ccb-9828-ef5293b38b04"))) ? ((30 * 60 * 1000) != (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("50b6fd70-c193-3d99-8789-cf7fabda9060"))) ? ((60 * 1000) > (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("0b82f225-b187-3e03-8634-f9bd50698ced"))) ? ((deleteNonceGracePeriod) != (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("99c91ede-8c30-35d3-b81c-575135a16461"))) ? ((30) != (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("74e4a677-229d-34f5-91bf-daa79dfc3d28"))) ? ((60 * 1000) == (60)) : (((KnobRuntime.check(java.util.UUID.fromString("c1b46d18-113a-3cb2-a18b-f1d5dd9df341"))) ? ((30) < (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("a2519be2-d9dd-356a-9dd2-f5b859c7d2f7"))) ? ((60) < (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("b93fe452-7d02-31a4-8b2f-93752e42b2c2"))) ? ((30 * 60) != (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("f3929621-ce60-3544-96b2-d643bf6ce471"))) ? ((60) >= ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("de9e6bbe-8259-3e55-a5ef-21fd65f85050"))) ? ((30 * 60) < (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("946ce9e5-e2bf-3467-a965-8b5625379471"))) ? ((30 * 60 * 1000) <= (60)) : (((KnobRuntime.check(java.util.UUID.fromString("dfc74f4e-f23f-32fe-8530-09d2096d4cd7"))) ? ((30 * 60 * 1000) > (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("fb861d1d-50ab-33f9-8efe-c319d88a2eb7"))) ? ((30 * 60 * 1000) == (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("6ef24979-2acb-31be-824f-b20521601764"))) ? ((30 * 60 * 1000) != (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("c292b5c0-069d-33a3-a1ef-1a4a531082a2"))) ? ((deleteNonceGracePeriod) != ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("51a26427-b74f-3e87-b41f-c3d48eb308d0"))) ? ((30 * 60) < (60)) : (((KnobRuntime.check(java.util.UUID.fromString("baaa9373-c4fa-33af-84d6-f4f7b729dfc3"))) ? ((60) > ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("f1cf4045-2a1a-3bef-bf8e-7920f0527037"))) ? ((30 * 60) <= (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("49727dfa-2a8a-36b5-b661-9473ae217ec8"))) ? ((deleteNonceGracePeriod) <= (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("3b748ae4-4e70-3091-ae9e-5d3be7f9c981"))) ? ((30 * 60) == (60)) : (((KnobRuntime.check(java.util.UUID.fromString("e9bd6a44-0794-3a99-bed4-7d7c4df49cf9"))) ? ((30 * 60) >= (60)) : (((KnobRuntime.check(java.util.UUID.fromString("0eb4021f-e42d-35df-b830-f054c66ea268"))) ? ((1000) > ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("015d7724-b3c9-31cd-a448-221511f828ae"))) ? ((deleteNonceGracePeriod) <= (60)) : (((KnobRuntime.check(java.util.UUID.fromString("67e57ba3-695d-3bc8-ad05-0246a72dabde"))) ? ((30 * 60 * 1000) > ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("3b700a87-b49c-3a5e-afe5-ccb59e3f42e1"))) ? ((30) == (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("93ff7e4f-8337-34f9-84b7-0f0e898bba3c"))) ? ((60 * 1000) > ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("6856242d-36c0-315e-8d3c-8153fba8b2d3"))) ? ((1000) != ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("ffcb0cdd-ad17-34ba-9793-ee5eeda51040"))) ? ((60 * 1000) == (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("d2e54558-d934-3a95-9256-52552e7c1189"))) ? ((30 * 60) <= ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("061eac93-54f8-3b54-ba26-a5dbbe8a64a1"))) ? ((60 * 1000) == ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("8a5b3381-dfdf-3ec4-9a69-725074e0fb9b"))) ? ((deleteNonceGracePeriod) >= (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("2969283a-aad8-3257-b002-897c4610ecb0"))) ? ((30 * 60 * 1000) <= ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("9d655f7f-1843-3d9d-b6d6-5f690447752d"))) ? ((1000) <= ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("efc59944-517c-3489-a773-eb7458542ba4"))) ? ((30 * 60 * 1000) < (60)) : (((KnobRuntime.check(java.util.UUID.fromString("fa10f54c-a937-3ad4-82f5-aa228a501e8b"))) ? ((60) == ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("9d74086c-3868-354e-b83a-e83df0419910"))) ? ((30 * 60) == ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("83cd1b33-0db9-326d-bcf8-3338b66f2b08"))) ? ((30 * 60 * 1000) == (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("307cd7e8-8401-3d8f-b58a-7e2daf949965"))) ? ((1000) != (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("5a8d8ca9-c4cf-3072-8f24-b4b8456d010b"))) ? ((deleteNonceGracePeriod) > ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("67ea770d-a60c-3cc3-a368-1523aa2e3e74"))) ? ((deleteNonceGracePeriod) != (60)) : (((KnobRuntime.check(java.util.UUID.fromString("d409c205-4919-372c-906c-281b188a76b4"))) ? ((30) <= (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("5bc5b56b-ab0c-3238-a4b4-366f383ca453"))) ? ((deleteNonceGracePeriod) == (60)) : (((KnobRuntime.check(java.util.UUID.fromString("534a142b-c9cd-3dec-b012-4afbbd6a5a2b"))) ? ((30 * 60 * 1000) < (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("0e93abd9-f792-3182-a043-66d3d925f75e"))) ? ((60 * 1000) < (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("fdc5be52-9cd5-339b-a2ea-248542d23553"))) ? ((30 * 60) != ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("628f0b0b-8c1a-3dd8-b2d7-ea8857ab8685"))) ? ((deleteNonceGracePeriod) < (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("8ec07b31-46e7-3ecc-b5cd-a4751d9ff854"))) ? ((deleteNonceGracePeriod) <= ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("329acb89-7e9b-38b6-b283-8d53d3252fde"))) ? ((30 * 60 * 1000) <= (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("a9ce7b6f-744a-3da4-bd8f-dac60eb15ea2"))) ? ((1000) > (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("ce8ff5fb-3fee-3459-bf15-fc6adc8ee00f"))) ? ((30 * 60 * 1000) < (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("427fc2e2-2a66-39c9-9371-e043878b8fe9"))) ? ((30 * 60) > (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("8233e2de-3c1c-3c52-8f51-3968969e69c6"))) ? ((60 * 1000) <= (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("de0d13ad-d0b4-327a-b6fa-02b7f939935b"))) ? ((deleteNonceGracePeriod) == ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("c8d4fa2e-9a91-3f69-af3f-c83cfe6d750b"))) ? ((60 * 1000) != (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("1b7823e7-701d-3497-a39b-329042b5fa41"))) ? ((deleteNonceGracePeriod) != (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("41d95218-41b8-3d94-80e0-8032dfa5bb6d"))) ? ((30 * 60) >= (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("b2218e82-66e6-3cee-bfe1-c6141cd08170"))) ? ((30 * 60 * 1000) != (60)) : (((KnobRuntime.check(java.util.UUID.fromString("b54bf55b-ed26-3223-b37a-b8f02979df0d"))) ? ((deleteNonceGracePeriod) == (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("9c662ea5-7361-3e03-b7dc-b93e016e4a7a"))) ? ((60 * 1000) >= (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("5884da95-54ac-38aa-a3bc-ab87ee61a3b1"))) ? ((30 * 60) > (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("6c6afbcc-2e94-3e17-a6f8-544df039b324"))) ? ((30 * 60 * 1000) == (60)) : (((KnobRuntime.check(java.util.UUID.fromString("68108900-4abe-3fcf-91c9-8a69dcdaad54"))) ? ((30 * 60 * 1000) > (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("1d3d4eba-63d9-344b-b550-23e700b17b1a"))) ? ((deleteNonceGracePeriod) < (60)) : (((KnobRuntime.check(java.util.UUID.fromString("89a33cb5-0a8b-3a42-b8ee-e4d41c2e8f9a"))) ? ((1000) == ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("72d97c51-d0fb-37b9-977b-0740950b99af"))) ? ((1000) >= (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("e91d2b98-239e-3428-81dc-1d066661a3dd"))) ? ((30 * 60) == (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("4b1d9354-5a02-38f8-a82a-bf6186923231"))) ? ((60 * 1000) >= (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("97d68223-b4bc-37a8-8861-3b06bf7d89ee"))) ? ((30 * 60 * 1000) >= (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("92d67415-b8f5-347c-8be7-3e3d32ebddb0"))) ? ((60 * 1000) >= (60)) : (((KnobRuntime.check(java.util.UUID.fromString("b06a8ed7-8190-3693-a2db-b4e593353e26"))) ? ((60) == (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("7c315d39-3084-351b-810a-1a6d058d4048"))) ? ((60) < ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("aa916d5f-f7bc-3af9-b089-3aa622f70b09"))) ? ((60 * 1000) >= ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("ac315d8e-b348-3ebd-bb26-b2f8da79f7bd"))) ? ((30 * 60) < ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("6e283952-7bef-3cc8-9070-9ada00e675bd"))) ? ((30 * 60) >= (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("ef4b9bca-fc7d-3dd3-9c93-43d842fdb42d"))) ? ((60) <= (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("b72617f5-42a0-3230-85b3-086013a6c250"))) ? ((30 * 60) >= ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("f5c17c3f-53ef-3d95-9e78-9674d928cfd2"))) ? ((30 * 60) > ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("c1ae16c0-779c-3213-bfa9-14b1c239ac94"))) ? ((30 * 60) != (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("a6c94ef6-2e12-3788-bda3-f9be4a5c9336"))) ? ((deleteNonceGracePeriod) > (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("cc81f001-d5d8-3250-8e84-081112112c7c"))) ? ((deleteNonceGracePeriod) >= ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("028eea33-99a0-30c7-a02b-e84a27dd03ba"))) ? ((30 * 60) == (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("0289b851-c16c-3054-a8e0-fc4f8446a904"))) ? ((30 * 60 * 1000) >= ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("5e732b1a-5cbb-3f20-bab0-35965aca38ec"))) ? ((60) >= (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("4e5bbad2-5181-3183-8a0d-91caef2bbcab"))) ? ((30) == ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("ded6c877-d96e-3f08-9883-2f19697021d5"))) ? ((30 * 60) != (60)) : (((KnobRuntime.check(java.util.UUID.fromString("a80e0326-0cff-3ccc-9a58-69b72b13bb24"))) ? ((deleteNonceGracePeriod) >= (60)) : (((KnobRuntime.check(java.util.UUID.fromString("f44bff83-5c8a-3e00-9a42-be18f0d66813"))) ? ((1000) == (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("f5073748-b1a6-3360-8a62-547163706d19"))) ? ((30 * 60) > (60)) : (((KnobRuntime.check(java.util.UUID.fromString("79be840e-7578-343c-b133-066f5d61ea5c"))) ? ((30 * 60 * 1000) > (60)) : (((KnobRuntime.check(java.util.UUID.fromString("16967769-499a-3810-880d-626c17095867"))) ? ((60 * 1000) < (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("03df696c-024f-3490-b4c1-b0357bfc82a6"))) ? ((deleteNonceGracePeriod) < ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("2d90b6b1-f573-3f0b-a0c7-c9afe906c290"))) ? ((60 * 1000) > (60)) : (((KnobRuntime.check(java.util.UUID.fromString("9b0b2fe0-a5f7-3ab6-b037-0ed982008bd6"))) ? ((30) >= ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("fcb01a3a-ceae-33a7-a412-88737e6e3af4"))) ? ((60 * 1000) != (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("0fd6785c-cdbd-3909-9d81-2b942081f4b1"))) ? ((60 * 1000) != ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("d1699779-a824-3358-9d67-236711b0ca27"))) ? ((1000) < (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("551093ab-dd05-3c3d-9c59-1a976b22bce5"))) ? ((30 * 60 * 1000) >= (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("3cf3baeb-bdb1-380d-9dc9-a184d2de2d6c"))) ? ((30) >= (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("52dcd047-7711-36b1-b37f-ebb10e9e0481"))) ? ((60 * 1000) <= ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("89ee9c52-66a6-3119-baec-79c7c56646b5"))) ? ((30 * 60 * 1000) == ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("cb3e92c5-ab7d-36ba-8ab0-c541b51729b0"))) ? ((30) > (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("5340c02f-f233-3016-ae6b-6c6ac258270f"))) ? ((30 * 60 * 1000) < ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("83607b75-d401-37ea-832a-4b3b8ff2d14f"))) ? ((deleteNonceGracePeriod) <= (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("9b609712-abcf-3d1d-ae69-53d35d952c6d"))) ? ((60 * 1000) == (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("af5e0463-9ae2-3cd3-b9d0-9718dd463b05"))) ? ((deleteNonceGracePeriod) < (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("fe3b89f7-5932-3168-8126-c047bbd96efd"))) ? ((1000) < ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("b157f81d-44f7-3113-b3b8-861ce4922651"))) ? ((60 * 1000) <= (60)) : (((KnobRuntime.check(java.util.UUID.fromString("6879efe0-71db-30a7-a457-60960a35ebb8"))) ? ((30 * 60 * 1000) <= (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("6395b593-fda7-37ba-bf83-91dd6cb90ed0"))) ? ((deleteNonceGracePeriod) == (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("3d4b4bf2-e0c8-3ef8-8a41-475787623881"))) ? ((60) <= ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("ebbb4734-ea59-387f-816f-1855e17060cf"))) ? ((30 * 60) <= (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("feb2e8b1-660c-31cd-a306-b79f9e38e756"))) ? ((deleteNonceGracePeriod) > (60)) : (((KnobRuntime.check(java.util.UUID.fromString("a7dbc765-d3af-3411-ada1-f83716cf5c56"))) ? ((60) != (60 * 1000)) : (((KnobRuntime.check(java.util.UUID.fromString("c7f4500f-1a5b-31c4-9844-54c05bef43fa"))) ? ((60 * 1000) < (60)) : (((KnobRuntime.check(java.util.UUID.fromString("d1438939-e618-3c61-a1c3-a959b08401ee"))) ? ((deleteNonceGracePeriod) >= (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("d71641b3-cd57-3c2f-8789-9845217f18c8"))) ? ((60 * 1000) < ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("7ba350f8-c31f-340d-bb28-d9b30c3bc374"))) ? ((30) != ((60) * (1000))) : (((KnobRuntime.check(java.util.UUID.fromString("a5d28253-6b36-3ffb-8195-b515393cf6ee"))) ? ((30 * 60 * 1000) >= (60)) : (((KnobRuntime.check(java.util.UUID.fromString("8d05fd47-5f9a-3542-b5eb-fd33d3b20fbe"))) ? ((deleteNonceGracePeriod) > (1000)) : (((KnobRuntime.check(java.util.UUID.fromString("bf6ddf50-fcb8-3869-996e-8ed99ed18e36"))) ? ((1000) <= (60 * 1000)) : (deleteNonceGracePeriod < 60 * 1000))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      LOG.warn("Nonce grace period " + deleteNonceGracePeriod
        + " is less than a minute; might be too small to be useful");
    }
  }

  public void setConflictWaitIterationMs(int conflictWaitIterationMs) {
    this.conflictWaitIterationMs = conflictWaitIterationMs;
  }

  /**
   * Starts the operation if operation with such nonce has not already succeeded. If the operation
   * is in progress, waits for it to end and checks whether it has succeeded.
   * @param group     Nonce group.
   * @param nonce     Nonce.
   * @param stoppable Stoppable that terminates waiting (if any) when the server is stopped.
   * @return true if the operation has not already succeeded and can proceed; false otherwise.
   */
  public boolean startOperation(long group, long nonce, Stoppable stoppable)
    throws InterruptedException {
    if (nonce == HConstants.NO_NONCE) return true;
    NonceKey nk = new NonceKey(group, nonce);
    OperationContext ctx = new OperationContext();
    while (true) {
      OperationContext oldResult = nonces.putIfAbsent(nk, ctx);
      if (oldResult == null) return true;

      // Collision with some operation - should be extremely rare.
      synchronized (oldResult) {
        int oldState = oldResult.getState();
        LOG.debug("Conflict detected by nonce: " + nk + ", " + oldResult);
        if (oldState != OperationContext.WAIT) {
          return oldState == OperationContext.PROCEED; // operation ended
        }
        oldResult.setHasWait();
        oldResult.wait(this.conflictWaitIterationMs); // operation is still active... wait and loop
        if (stoppable.isStopped()) {
          throw new InterruptedException("Server stopped");
        }
      }
    }
  }

  /**
   * Ends the operation started by startOperation.
   * @param group   Nonce group.
   * @param nonce   Nonce.
   * @param success Whether the operation has succeeded.
   */
  public void endOperation(long group, long nonce, boolean success) {
    if (nonce == HConstants.NO_NONCE) return;
    NonceKey nk = new NonceKey(group, nonce);
    OperationContext newResult = nonces.get(nk);
    assert newResult != null;
    synchronized (newResult) {
      assert newResult.getState() == OperationContext.WAIT;
      // If we failed, other retries can proceed.
      newResult.setState(success ? OperationContext.DONT_PROCEED : OperationContext.PROCEED);
      if (success) {
        newResult.reportActivity(); // Set time to use for cleanup.
      } else {
        OperationContext val = nonces.remove(nk);
        assert val == newResult;
      }
      if (newResult.hasWait()) {
        LOG.debug("Conflict with running op ended: " + nk + ", " + newResult);
        newResult.notifyAll();
      }
    }
  }

  /**
   * Store the write point in OperationContext when the operation succeed.
   * @param group Nonce group.
   * @param nonce Nonce.
   * @param mvcc  Write point of the succeed operation.
   */
  public void addMvccToOperationContext(long group, long nonce, long mvcc) {
    if (nonce == HConstants.NO_NONCE) {
      return;
    }
    NonceKey nk = new NonceKey(group, nonce);
    OperationContext result = nonces.get(nk);
    assert result != null;
    synchronized (result) {
      result.setMvcc(mvcc);
    }
  }

  /**
   * Return the write point of the previous succeed operation.
   * @param group Nonce group.
   * @param nonce Nonce.
   * @return write point of the previous succeed operation.
   */
  public long getMvccFromOperationContext(long group, long nonce) {
    if (nonce == HConstants.NO_NONCE) {
      return Long.MAX_VALUE;
    }
    NonceKey nk = new NonceKey(group, nonce);
    OperationContext result = nonces.get(nk);
    return result == null ? Long.MAX_VALUE : result.getMvcc();
  }

  /**
   * Reports the operation from WAL during replay.
   * @param group     Nonce group.
   * @param nonce     Nonce.
   * @param writeTime Entry write time, used to ignore entries that are too old.
   */
  public void reportOperationFromWal(long group, long nonce, long writeTime) {
    if (nonce == HConstants.NO_NONCE) return;
    // Give the write time some slack in case the clocks are not synchronized.
    long now = EnvironmentEdgeManager.currentTime();
    if (now > writeTime + (deleteNonceGracePeriod * 1.5)) return;
    OperationContext newResult = new OperationContext();
    newResult.setState(OperationContext.DONT_PROCEED);
    NonceKey nk = new NonceKey(group, nonce);
    OperationContext oldResult = nonces.putIfAbsent(nk, newResult);
    if (oldResult != null) {
      // Some schemes can have collisions (for example, expiring hashes), so just log it.
      // We have no idea about the semantics here, so this is the least of many evils.
      LOG.warn(
        "Nonce collision during WAL recovery: " + nk + ", " + oldResult + " with " + newResult);
    }
  }

  /**
   * Creates a scheduled chore that is used to clean up old nonces.
   * @param stoppable Stoppable for the chore.
   * @return ScheduledChore; the scheduled chore is not started.
   */
  public ScheduledChore createCleanupScheduledChore(Stoppable stoppable) {
    // By default, it will run every 6 minutes (30 / 5).
    return new ScheduledChore("nonceCleaner", stoppable, deleteNonceGracePeriod / 5) {
      @Override
      protected void chore() {
        cleanUpOldNonces();
      }
    };
  }

  private void cleanUpOldNonces() {
    long cutoff = EnvironmentEdgeManager.currentTime() - deleteNonceGracePeriod;
    for (Map.Entry<NonceKey, OperationContext> entry : nonces.entrySet()) {
      OperationContext oc = entry.getValue();
      if (!oc.isExpired(cutoff)) continue;
      synchronized (oc) {
        if (oc.getState() == OperationContext.WAIT || !oc.isExpired(cutoff)) continue;
        nonces.remove(entry.getKey());
      }
    }
  }
}
