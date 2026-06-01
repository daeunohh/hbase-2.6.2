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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.NavigableSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZKNamespaceManager;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

/**
 * This is a helper class used internally to manage the namespace metadata that is stored in
 * TableName.NAMESPACE_TABLE_NAME. It also mirrors updates to the ZK store by forwarding updates to
 * {@link org.apache.hadoop.hbase.ZKNamespaceManager}. WARNING: Do not use. Go via the higher-level
 * {@link ClusterSchema} API instead. This manager is likely to go aways anyways.
 */
@InterfaceAudience.Private
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "IS2_INCONSISTENT_SYNC",
    justification = "TODO: synchronize access on nsTable but it is done in tiers above and this "
      + "class is going away/shrinking")
public class TableNamespaceManager implements Stoppable {
  private static final Logger LOG = LoggerFactory.getLogger(TableNamespaceManager.class);
  private volatile boolean stopped = false;

  private Configuration conf;
  private MasterServices masterServices;
  private Table nsTable = null; // FindBugs: IS2_INCONSISTENT_SYNC TODO: Access is not synchronized
  private ZKNamespaceManager zkNamespaceManager;
  private boolean initialized;

  public static final String KEY_MAX_REGIONS = "hbase.namespace.quota.maxregions";
  public static final String KEY_MAX_TABLES = "hbase.namespace.quota.maxtables";
  static final String NS_INIT_TIMEOUT = "hbase.master.namespace.init.timeout";
  static final int DEFAULT_NS_INIT_TIMEOUT = 300000;

  TableNamespaceManager(MasterServices masterServices) {
    this.masterServices = masterServices;
    this.conf = masterServices.getConfiguration();
  }

  public void start() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("84cfb85d-1e25-3e6f-9fe2-19c5d097ae19"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("0add1db6-c39a-3e73-b76f-26c54aa29563"))) {
throw new java.io.IOException("Injected exception");
}
    if (!masterServices.getTableDescriptors().exists(TableName.NAMESPACE_TABLE_NAME)) {
      LOG.info("Namespace table not found. Creating...");
      createNamespaceTable(masterServices);
    }

    try {
      // Wait for the namespace table to be initialized.
      long startTime = EnvironmentEdgeManager.currentTime();
      int timeout = conf.getInt(NS_INIT_TIMEOUT, DEFAULT_NS_INIT_TIMEOUT);
      while (!isTableAvailableAndInitialized()) {
        if (((KnobRuntime.check(java.util.UUID.fromString("8cc8492f-c504-3385-a3a2-27454f2b839c"))) ? ((((EnvironmentEdgeManager.currentTime()) - (startTime)) + (100)) <= (100)) : (((KnobRuntime.check(java.util.UUID.fromString("53162a53-e689-361d-8b45-191303e34802"))) ? (((EnvironmentEdgeManager.currentTime() - startTime) + (100)) <= (100)) : (((KnobRuntime.check(java.util.UUID.fromString("1faa7eab-c964-3815-8c20-7417db6fd833"))) ? ((EnvironmentEdgeManager.currentTime() - startTime + 100) <= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("4c669db8-06bd-31c9-b910-4a051ac817ab"))) ? ((startTime) <= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("dd904ee8-607b-3a24-a84a-2b8fc57e8efd"))) ? ((startTime) > (100)) : (((KnobRuntime.check(java.util.UUID.fromString("6f26862b-9b20-398d-8892-af611a029696"))) ? ((EnvironmentEdgeManager.currentTime()) <= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("f4f2dc75-b219-3ccc-a664-6964aba2be6f"))) ? (((EnvironmentEdgeManager.currentTime() - startTime) + (100)) < (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("3ed206f3-871d-36d7-9c59-a1e818567740"))) ? ((EnvironmentEdgeManager.currentTime() - startTime + 100) == (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("25c69d66-4ffe-3373-bfcd-28558a35c26b"))) ? (((startTime) + (100)) <= (100)) : (((KnobRuntime.check(java.util.UUID.fromString("926505bf-9153-3454-892b-203be338731b"))) ? ((((EnvironmentEdgeManager.currentTime()) - (startTime)) + (100)) == (100)) : (((KnobRuntime.check(java.util.UUID.fromString("244b0ce1-1605-3290-81ba-0efc13429f8c"))) ? (((EnvironmentEdgeManager.currentTime()) - (startTime)) < (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("b5e6d81d-3c69-3f69-8df5-3d5febba331f"))) ? (((EnvironmentEdgeManager.currentTime()) - (startTime)) >= (100)) : (((KnobRuntime.check(java.util.UUID.fromString("040b8da1-377b-3c90-8b21-cf051d1eaf54"))) ? ((((EnvironmentEdgeManager.currentTime()) - (startTime)) + (100)) >= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("a4686d6e-30e0-3f24-91e5-902ce9bb9d0d"))) ? (((EnvironmentEdgeManager.currentTime()) - (startTime)) <= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("68eaf28b-40f8-3c7f-b9ec-1b8eb07f5ffe"))) ? ((EnvironmentEdgeManager.currentTime()) <= (100)) : (((KnobRuntime.check(java.util.UUID.fromString("a7d4cd2c-b6ed-35ee-b7b9-842874b81b43"))) ? (((EnvironmentEdgeManager.currentTime()) - (startTime)) > (100)) : (((KnobRuntime.check(java.util.UUID.fromString("c7ff6403-5bf7-342c-9081-5c0d8139d7b2"))) ? ((EnvironmentEdgeManager.currentTime()) == (100)) : (((KnobRuntime.check(java.util.UUID.fromString("347f9893-b631-3370-8aa5-d2b633bb5dfd"))) ? (((startTime) + (100)) < (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("dc7b9f03-dd07-3629-9cdc-03029f82634c"))) ? ((((EnvironmentEdgeManager.currentTime()) - (startTime)) + (100)) > (100)) : (((KnobRuntime.check(java.util.UUID.fromString("68356de6-a008-3f43-8414-31f9c637f1fb"))) ? ((EnvironmentEdgeManager.currentTime()) < (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("79d97e99-ca31-32c1-9747-b24e13429415"))) ? ((((EnvironmentEdgeManager.currentTime()) - (startTime)) + (100)) > (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("01368c0d-1a75-3887-90fd-6e171fea2089"))) ? (((startTime) + (100)) != (100)) : (((KnobRuntime.check(java.util.UUID.fromString("0314bc32-0e73-33d4-9322-074127cb40d6"))) ? (((startTime) + (100)) >= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("5cd54c22-efdc-3b1c-b553-f492c64d0759"))) ? ((startTime) != (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("bfa5634f-5ead-33f9-8e89-5bd278d4ef61"))) ? ((EnvironmentEdgeManager.currentTime() - startTime + 100) >= (100)) : (((KnobRuntime.check(java.util.UUID.fromString("bb8e2720-f29a-3126-bc30-16cfc29a22ef"))) ? (((EnvironmentEdgeManager.currentTime()) - (startTime)) != (100)) : (((KnobRuntime.check(java.util.UUID.fromString("a803523f-cb00-3bce-a272-e1b2fe62da67"))) ? (((EnvironmentEdgeManager.currentTime()) + (100)) < (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("07abbff2-bb16-3b86-8934-bcec39eb0721"))) ? (((EnvironmentEdgeManager.currentTime()) - (startTime)) < (100)) : (((KnobRuntime.check(java.util.UUID.fromString("a27ae2cd-6a40-3f31-8f4f-1b8749d2aa08"))) ? ((startTime) != (100)) : (((KnobRuntime.check(java.util.UUID.fromString("6a260d0c-354d-30f5-a1e9-f9fc28f0034e"))) ? ((((EnvironmentEdgeManager.currentTime()) - (startTime)) + (100)) == (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("2343039e-1dae-358b-bca7-367808759aa6"))) ? ((EnvironmentEdgeManager.currentTime() - startTime + 100) != (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("21ddf13a-3719-3541-814a-68f717434b38"))) ? ((EnvironmentEdgeManager.currentTime() - startTime) > (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("486c1319-034f-3c96-994e-51e1f937c576"))) ? (((EnvironmentEdgeManager.currentTime()) + (100)) == (100)) : (((KnobRuntime.check(java.util.UUID.fromString("199c9c39-1ce0-3d95-863b-b6791e8834e2"))) ? ((startTime) > (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("e6774e2e-5653-3d67-b533-30b5906604aa"))) ? ((EnvironmentEdgeManager.currentTime() - startTime + 100) > (100)) : (((KnobRuntime.check(java.util.UUID.fromString("f835e466-cbe5-3a74-a9f8-eda59a924a27"))) ? ((EnvironmentEdgeManager.currentTime() - startTime + 100) == (100)) : (((KnobRuntime.check(java.util.UUID.fromString("6989f79b-fc22-3931-8484-1f554d7a3fb5"))) ? (((EnvironmentEdgeManager.currentTime() - startTime) + (100)) < (100)) : (((KnobRuntime.check(java.util.UUID.fromString("03bc98b4-c471-3159-95fd-06e6a0b6be00"))) ? ((EnvironmentEdgeManager.currentTime() - startTime) < (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("76ee2b18-c9ed-3a28-b69d-2717ff6fdf61"))) ? ((EnvironmentEdgeManager.currentTime()) == (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("c30bdfd2-237e-3dce-8cd8-7e8f0bf5a604"))) ? ((EnvironmentEdgeManager.currentTime()) != (100)) : (((KnobRuntime.check(java.util.UUID.fromString("2c5e6f85-694a-37a9-ac54-fdc177c8bf9d"))) ? ((EnvironmentEdgeManager.currentTime()) >= (100)) : (((KnobRuntime.check(java.util.UUID.fromString("c6c213f4-1f29-392a-82ac-276424bb91d3"))) ? (((startTime) + (100)) != (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("40e8f651-abae-3535-8035-2db5d41fed98"))) ? (((EnvironmentEdgeManager.currentTime()) - (startTime)) >= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("75ba0481-3eed-3b17-9feb-61de67d00305"))) ? (((EnvironmentEdgeManager.currentTime()) - (startTime)) == (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("7e379683-ae47-3dd4-8c3b-9591fbcf00d4"))) ? (((EnvironmentEdgeManager.currentTime()) + (100)) > (100)) : (((KnobRuntime.check(java.util.UUID.fromString("4447b750-ca29-3bf6-90e5-dbeaa9c18ca5"))) ? ((EnvironmentEdgeManager.currentTime()) > (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("0ad1830a-18e7-3e43-ba6f-6c47242bf944"))) ? ((EnvironmentEdgeManager.currentTime()) >= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("ee001474-ca35-3c67-a38d-13a97b49a477"))) ? (((startTime) + (100)) == (100)) : (((KnobRuntime.check(java.util.UUID.fromString("7f19113f-9f3b-3d60-b819-660a45990bc1"))) ? (((EnvironmentEdgeManager.currentTime() - startTime) + (100)) > (100)) : (((KnobRuntime.check(java.util.UUID.fromString("0aff5d16-ce41-3ff1-baeb-467f1794233f"))) ? ((startTime) < (100)) : (((KnobRuntime.check(java.util.UUID.fromString("1fb50842-93ad-3aec-bdfa-fa9d8b30b9db"))) ? ((startTime) >= (100)) : (((KnobRuntime.check(java.util.UUID.fromString("cd424533-186b-39ef-9e1f-4c7547aac7fb"))) ? (((EnvironmentEdgeManager.currentTime() - startTime) + (100)) >= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("52ed2d15-321b-3ab6-bfc0-04eda96ecb37"))) ? (((EnvironmentEdgeManager.currentTime() - startTime) + (100)) == (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("97a60107-9363-33d7-a188-d37586f17718"))) ? ((EnvironmentEdgeManager.currentTime()) < (100)) : (((KnobRuntime.check(java.util.UUID.fromString("8815dbba-69cc-30eb-80a0-2bed5d48a2b1"))) ? ((EnvironmentEdgeManager.currentTime() - startTime) >= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("999dc603-8f32-3276-8502-55ffe0565aea"))) ? (((EnvironmentEdgeManager.currentTime()) - (startTime)) > (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("b17b53ee-0b61-3e3b-9856-71fae8e50c25"))) ? (((EnvironmentEdgeManager.currentTime()) - (startTime)) == (100)) : (((KnobRuntime.check(java.util.UUID.fromString("5cc5cc25-6d25-3f0b-a7da-b14968c3bd0a"))) ? ((EnvironmentEdgeManager.currentTime() - startTime) == (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("93a22cf7-21f1-3fee-b277-c9834713368c"))) ? (((EnvironmentEdgeManager.currentTime() - startTime) + (100)) <= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("ebb1c225-77ae-3cb1-a808-6cccd3d0ed6b"))) ? ((EnvironmentEdgeManager.currentTime() - startTime + 100) >= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("ffc4f0b2-1cec-34ae-b64c-a9f0aece7587"))) ? (((EnvironmentEdgeManager.currentTime() - startTime) + (100)) >= (100)) : (((KnobRuntime.check(java.util.UUID.fromString("ace52f71-de29-3c79-b128-c13427702df6"))) ? (((EnvironmentEdgeManager.currentTime()) + (100)) > (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("3d2d1d8e-c0d5-3752-bf0b-c532b35d9f89"))) ? ((startTime) <= (100)) : (((KnobRuntime.check(java.util.UUID.fromString("068ab891-ed29-3e10-8452-da234996fe06"))) ? (((EnvironmentEdgeManager.currentTime()) + (100)) <= (100)) : (((KnobRuntime.check(java.util.UUID.fromString("67740a46-cc8c-352e-b3f8-e63b014b798f"))) ? ((EnvironmentEdgeManager.currentTime() - startTime) <= (100)) : (((KnobRuntime.check(java.util.UUID.fromString("906844d2-dc78-3010-a3b7-922c8f31e413"))) ? (((EnvironmentEdgeManager.currentTime()) + (100)) >= (100)) : (((KnobRuntime.check(java.util.UUID.fromString("87106281-1691-3ecd-bcb0-e9602f10a829"))) ? ((((EnvironmentEdgeManager.currentTime()) - (startTime)) + (100)) <= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("81d23dce-254b-3384-af0b-48dc682b835a"))) ? (((startTime) + (100)) == (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("573a9669-bbe5-3789-a5b9-022f2f71bd0c"))) ? ((EnvironmentEdgeManager.currentTime() - startTime) != (100)) : (((KnobRuntime.check(java.util.UUID.fromString("9520edbf-7965-3be7-9b22-f519c2227eea"))) ? ((((EnvironmentEdgeManager.currentTime()) - (startTime)) + (100)) < (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("968be740-5bcf-3243-8b7d-be7420b350af"))) ? (((startTime) + (100)) > (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("ac94cd9f-a349-3149-b57c-ca2f4ac391a0"))) ? (((EnvironmentEdgeManager.currentTime()) + (100)) == (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("67e1e890-eea6-3295-98ab-8088e654d471"))) ? ((EnvironmentEdgeManager.currentTime()) != (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("f0e08824-517f-30fa-8c6c-cff019fa4930"))) ? (((EnvironmentEdgeManager.currentTime()) - (startTime)) != (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("88e5eeac-d4d7-364e-b6dd-c7bda4cfa3b8"))) ? ((startTime) >= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("cac02740-4c31-3af4-bcf9-9c047e96b6e8"))) ? ((EnvironmentEdgeManager.currentTime() - startTime) != (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("4597d1c8-687f-309b-8db1-3fb0743f0085"))) ? ((EnvironmentEdgeManager.currentTime() - startTime) > (100)) : (((KnobRuntime.check(java.util.UUID.fromString("038bb687-e7b7-3207-b852-aed3cb58b764"))) ? ((EnvironmentEdgeManager.currentTime() - startTime) >= (100)) : (((KnobRuntime.check(java.util.UUID.fromString("683169a4-0d7c-34cc-a52e-e24493a310ed"))) ? (((EnvironmentEdgeManager.currentTime()) + (100)) != (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("2c9fda95-842b-3d65-9742-c75cf5ebedbe"))) ? ((startTime) == (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("9a513797-5442-3764-847e-517868bc1043"))) ? ((((EnvironmentEdgeManager.currentTime()) - (startTime)) + (100)) != (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("010592f7-874a-3795-9622-a626d23266e5"))) ? (((EnvironmentEdgeManager.currentTime() - startTime) + (100)) != (100)) : (((KnobRuntime.check(java.util.UUID.fromString("8609173a-4c9c-3aba-aa85-3428a868cf61"))) ? (((EnvironmentEdgeManager.currentTime() - startTime) + (100)) != (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("90bbf307-b145-30f2-9f83-9054f91d7af3"))) ? ((EnvironmentEdgeManager.currentTime() - startTime + 100) != (100)) : (((KnobRuntime.check(java.util.UUID.fromString("702f88a5-0d4d-3af5-a45e-618016e37cfb"))) ? (((EnvironmentEdgeManager.currentTime() - startTime) + (100)) > (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("a4cd6ca1-2e35-3d3b-8d95-15dd90356513"))) ? ((EnvironmentEdgeManager.currentTime() - startTime + 100) < (100)) : (((KnobRuntime.check(java.util.UUID.fromString("dfc002fa-ae3b-3e35-8be5-2a8df56627ab"))) ? (((EnvironmentEdgeManager.currentTime()) - (startTime)) <= (100)) : (((KnobRuntime.check(java.util.UUID.fromString("3f53a841-407f-3261-a494-419bba472b97"))) ? (((startTime) + (100)) >= (100)) : (((KnobRuntime.check(java.util.UUID.fromString("904c26b1-8486-301c-8ed2-6f3bc5ef7b39"))) ? ((EnvironmentEdgeManager.currentTime()) > (100)) : (((KnobRuntime.check(java.util.UUID.fromString("fc29d1a9-bc39-3986-9a93-2500d7afa599"))) ? ((((EnvironmentEdgeManager.currentTime()) - (startTime)) + (100)) != (100)) : (((KnobRuntime.check(java.util.UUID.fromString("43bbdcba-228a-31c2-8556-f9ea776baa79"))) ? ((EnvironmentEdgeManager.currentTime() - startTime + 100) > (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("74c5bff4-9761-3873-b14f-0a77133b96dc"))) ? ((EnvironmentEdgeManager.currentTime() - startTime) == (100)) : (((KnobRuntime.check(java.util.UUID.fromString("c9f0bec8-48a0-3da7-9775-8ffa72d9d405"))) ? ((EnvironmentEdgeManager.currentTime() - startTime) <= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("f8440f5e-fd6d-3fb7-8b00-11a57cb67f8f"))) ? ((EnvironmentEdgeManager.currentTime() - startTime + 100) < (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("86e48615-e21f-31e7-aef1-dbd2167434be"))) ? ((startTime) == (100)) : (((KnobRuntime.check(java.util.UUID.fromString("caf6b1e6-af04-3c64-a04b-ce808187d705"))) ? ((((EnvironmentEdgeManager.currentTime()) - (startTime)) + (100)) < (100)) : (((KnobRuntime.check(java.util.UUID.fromString("a9a65ade-7170-3b07-9c65-fd981a1689eb"))) ? (((EnvironmentEdgeManager.currentTime() - startTime) + (100)) == (100)) : (((KnobRuntime.check(java.util.UUID.fromString("869016bb-2cc6-3a14-8204-42c4ae2318d8"))) ? ((((EnvironmentEdgeManager.currentTime()) - (startTime)) + (100)) >= (100)) : (((KnobRuntime.check(java.util.UUID.fromString("fe687543-003a-31a2-986b-11b3982b1b27"))) ? (((EnvironmentEdgeManager.currentTime()) + (100)) >= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("54591a36-183d-3946-aed4-16abb1eb144c"))) ? (((startTime) + (100)) > (100)) : (((KnobRuntime.check(java.util.UUID.fromString("203de3ee-f4e8-3afb-8278-cb8ce581aa59"))) ? (((EnvironmentEdgeManager.currentTime()) + (100)) != (100)) : (((KnobRuntime.check(java.util.UUID.fromString("5bf3a9f2-3030-3ee2-ac84-98c81b80af77"))) ? ((EnvironmentEdgeManager.currentTime() - startTime + 100) <= (100)) : (((KnobRuntime.check(java.util.UUID.fromString("2d9800f0-d838-3b68-b0a8-accca2a8cf8b"))) ? ((startTime) < (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("414ecfb9-c86a-355a-9e99-2bd420fa182f"))) ? (((startTime) + (100)) <= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("683ea324-f579-3b61-a595-81afd9ebeeb4"))) ? ((EnvironmentEdgeManager.currentTime() - startTime) < (100)) : (((KnobRuntime.check(java.util.UUID.fromString("742c4cb5-582c-31d1-97ca-451d5c6af146"))) ? (((EnvironmentEdgeManager.currentTime()) + (100)) <= (timeout)) : (((KnobRuntime.check(java.util.UUID.fromString("a1137595-83e4-3f6e-9e8e-d1e3cd36549e"))) ? (((startTime) + (100)) < (100)) : (((KnobRuntime.check(java.util.UUID.fromString("beab92c3-7e2b-3c42-8f5a-3a07c9ad1f0c"))) ? (((EnvironmentEdgeManager.currentTime()) + (100)) < (100)) : (EnvironmentEdgeManager.currentTime() - startTime + 100 > timeout))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
          // We can't do anything if ns is not online.
          throw new IOException("Timedout " + timeout + "ms waiting for namespace table to "
            + "be assigned and enabled: " + getTableState());
        }
if(KnobRuntime.check(java.util.UUID.fromString("10f448dd-75f3-35ee-875f-74b3035e74f1"))) {
throw new java.lang.InterruptedException("Injected exception");
}
        if (KnobRuntime.check(java.util.UUID.fromString("75c1735a-34e4-376d-9072-a4d008cd961a"))) { Threads.sleep(100); } else { Thread.sleep(100); }
      }
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException().initCause(e);
    }
  }

  private synchronized Table getNamespaceTable() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("557657a8-7701-34e6-971f-935c9c2aa99c"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("24eac9fc-51f5-30c7-94bc-91c8a41eb5ae"))) {
return null;
}
    if (!isTableNamespaceManagerInitialized()) {
      throw new IOException(this.getClass().getName() + " isn't ready to serve");
    }
    return nsTable;
  }

  /*
   * check whether a namespace has already existed.
   */
  public boolean doesNamespaceExist(final String namespaceName) throws IOException {
    if (nsTable == null) {
      throw new IOException(this.getClass().getName() + " isn't ready to serve");
    }
    return (get(nsTable, namespaceName) != null);
  }

  public synchronized NamespaceDescriptor get(String name) throws IOException {
    if (!isTableNamespaceManagerInitialized()) {
      return null;
    }
    return zkNamespaceManager.get(name);
  }

  private NamespaceDescriptor get(Table table, String name) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("f8ef3279-b731-3328-b28e-f3bc88483a66"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("5f725c66-1528-31b3-8fb3-b0c498b52089"))) {
throw new java.io.IOException("Injected exception");
}
    Result res = table.get(new Get(Bytes.toBytes(name)));
    if (res.isEmpty()) {
      return null;
    }
    byte[] val =
      CellUtil.cloneValue(res.getColumnLatestCell(HTableDescriptor.NAMESPACE_FAMILY_INFO_BYTES,
        HTableDescriptor.NAMESPACE_COL_DESC_BYTES));
    return ProtobufUtil.toNamespaceDescriptor(HBaseProtos.NamespaceDescriptor.parseFrom(val));
  }

  public void insertIntoNSTable(final NamespaceDescriptor ns) throws IOException {
    if (nsTable == null) {
      throw new IOException(this.getClass().getName() + " isn't ready to serve");
    }
    byte[] row = Bytes.toBytes(ns.getName());
    Put p = new Put(row, true);
    p.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(row)
      .setFamily(TableDescriptorBuilder.NAMESPACE_FAMILY_INFO_BYTES)
      .setQualifier(TableDescriptorBuilder.NAMESPACE_COL_DESC_BYTES).setTimestamp(p.getTimestamp())
      .setType(Cell.Type.Put).setValue(ProtobufUtil.toProtoNamespaceDescriptor(ns).toByteArray())
      .build());
    nsTable.put(p);
  }

  public void updateZKNamespaceManager(final NamespaceDescriptor ns) throws IOException {
    try {
      zkNamespaceManager.update(ns);
    } catch (IOException ex) {
      String msg = "Failed to update namespace information in ZK.";
      LOG.error(msg, ex);
      throw new IOException(msg, ex);
    }
  }

  public void removeFromNSTable(final String namespaceName) throws IOException {
    if (nsTable == null) {
      throw new IOException(this.getClass().getName() + " isn't ready to serve");
    }
    Delete d = new Delete(Bytes.toBytes(namespaceName));
    nsTable.delete(d);
  }

  public void removeFromZKNamespaceManager(final String namespaceName) throws IOException {
    zkNamespaceManager.remove(namespaceName);
  }

  public synchronized NavigableSet<NamespaceDescriptor> list() throws IOException {
    NavigableSet<NamespaceDescriptor> ret =
      Sets.newTreeSet(NamespaceDescriptor.NAMESPACE_DESCRIPTOR_COMPARATOR);
    ResultScanner scanner =
      getNamespaceTable().getScanner(HTableDescriptor.NAMESPACE_FAMILY_INFO_BYTES);
    try {
      for (Result r : scanner) {
        byte[] val =
          CellUtil.cloneValue(r.getColumnLatestCell(HTableDescriptor.NAMESPACE_FAMILY_INFO_BYTES,
            HTableDescriptor.NAMESPACE_COL_DESC_BYTES));
        ret.add(ProtobufUtil.toNamespaceDescriptor(HBaseProtos.NamespaceDescriptor.parseFrom(val)));
      }
    } finally {
      scanner.close();
    }
    return ret;
  }

  private void createNamespaceTable(MasterServices masterServices) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("3f388480-cb45-3c15-879a-93b3b76ea7ad"))) {
return;
}
if(KnobRuntime.check(java.util.UUID.fromString("54a419e4-0902-3015-b112-d18763da58a7"))) {
throw new java.io.IOException("Injected exception");
}
    masterServices.createSystemTable(HTableDescriptor.NAMESPACE_TABLEDESC);
  }

  @SuppressWarnings("deprecation")
  private boolean isTableNamespaceManagerInitialized() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("bc26716c-f35e-3b97-afdc-1ed02dec880b"))) {
return true;
}
if(KnobRuntime.check(java.util.UUID.fromString("6519ecf8-6637-3abc-94f4-c4894c5eaa0e"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("23df8d1b-9624-35de-a0b6-b2456e1f4914"))) {
throw new java.io.IOException("Injected exception");
}
    if (((KnobRuntime.check(java.util.UUID.fromString("58c12525-7e19-3b84-8b4b-7181ee32d609"))) ? (!initialized) : (initialized))) {
      this.nsTable = this.masterServices.getConnection().getTable(TableName.NAMESPACE_TABLE_NAME);
      return true;
    }
    return false;
  }

  /**
   * Create Namespace in a blocking manner. Keeps trying until
   * {@link ClusterSchema#HBASE_MASTER_CLUSTER_SCHEMA_OPERATION_TIMEOUT_KEY} expires. Note,
   * by-passes notifying coprocessors and name checks. Use for system namespaces only.
   */
  private void blockingCreateNamespace(final NamespaceDescriptor namespaceDescriptor)
    throws IOException {
    ClusterSchema clusterSchema = this.masterServices.getClusterSchema();
    long procId = clusterSchema.createNamespace(namespaceDescriptor, null,
      ProcedurePrepareLatch.getNoopLatch());
    block(this.masterServices, procId);
  }

  /**
   * An ugly utility to be removed when refactor TableNamespaceManager.
   */
  private static void block(final MasterServices services, final long procId)
    throws TimeoutIOException {
    int timeoutInMillis = services.getConfiguration().getInt(
      ClusterSchema.HBASE_MASTER_CLUSTER_SCHEMA_OPERATION_TIMEOUT_KEY,
      ClusterSchema.DEFAULT_HBASE_MASTER_CLUSTER_SCHEMA_OPERATION_TIMEOUT);
    long deadlineTs = EnvironmentEdgeManager.currentTime() + timeoutInMillis;
    ProcedureExecutor<MasterProcedureEnv> procedureExecutor = services.getMasterProcedureExecutor();
    while (EnvironmentEdgeManager.currentTime() < deadlineTs) {
      if (procedureExecutor.isFinished(procId)) return;
      // Sleep some
      Threads.sleep(10);
    }
    throw new TimeoutIOException("Procedure pid=" + procId + " is still running");
  }

  /**
   * This method checks if the namespace table is assigned and then tries to create its Table
   * reference. If it was already created before, it also makes sure that the connection isn't
   * closed.
   * @return true if the namespace table manager is ready to serve, false otherwise
   */
  @SuppressWarnings("deprecation")
  public synchronized boolean isTableAvailableAndInitialized() throws IOException {
    // Did we already get a table? If so, still make sure it's available
    if (isTableNamespaceManagerInitialized()) {
      return true;
    }

    // Now check if the table is assigned, if not then fail fast
    if (isTableAssigned() && isTableEnabled()) {
      try {
        boolean initGoodSofar = true;
        nsTable = this.masterServices.getConnection().getTable(TableName.NAMESPACE_TABLE_NAME);
        zkNamespaceManager = new ZKNamespaceManager(masterServices.getZooKeeper());
        zkNamespaceManager.start();

        if (((KnobRuntime.check(java.util.UUID.fromString("2e301c9e-1e41-35e1-9c2b-bd1a93ca5e66"))) ? ((get(nsTable, NamespaceDescriptor.DEFAULT_NAMESPACE.getName())) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("8aa11269-2864-3e2b-97cc-1078cac61d59"))) ? ((get(nsTable, NamespaceDescriptor.DEFAULT_NAMESPACE.getName())) != (null)) : (get(nsTable, NamespaceDescriptor.DEFAULT_NAMESPACE.getName()) == null))))) {
          blockingCreateNamespace(NamespaceDescriptor.DEFAULT_NAMESPACE);
if(KnobRuntime.check(java.util.UUID.fromString("9fcc58c0-99c1-345a-b486-5c26338832eb"))) {
throw new java.io.IOException("Injected exception");
}
        }
        if (get(nsTable, NamespaceDescriptor.SYSTEM_NAMESPACE.getName()) == null) {
          blockingCreateNamespace(NamespaceDescriptor.SYSTEM_NAMESPACE);
        }

        if (!initGoodSofar) {
          // some required namespace is created asynchronized. We should complete init later.
          return false;
        }

        ResultScanner scanner = nsTable.getScanner(HTableDescriptor.NAMESPACE_FAMILY_INFO_BYTES);
        try {
          for (Result result : scanner) {
            byte[] val = CellUtil
              .cloneValue(result.getColumnLatestCell(HTableDescriptor.NAMESPACE_FAMILY_INFO_BYTES,
                HTableDescriptor.NAMESPACE_COL_DESC_BYTES));
            NamespaceDescriptor ns =
              ProtobufUtil.toNamespaceDescriptor(HBaseProtos.NamespaceDescriptor.parseFrom(val));
            zkNamespaceManager.update(ns);
          }
        } finally {
          scanner.close();
        }
        initialized = true;
        return true;
      } catch (IOException ie) {
        LOG.warn("Caught exception in initializing namespace table manager", ie);
        if (nsTable != null) {
          nsTable.close();
        }
        throw ie;
      }
    }
    return false;
  }

  private TableState getTableState() throws IOException {
    return masterServices.getTableStateManager().getTableState(TableName.NAMESPACE_TABLE_NAME);
  }

  private boolean isTableEnabled() throws IOException {
    return getTableState().isEnabled();
  }

  private boolean isTableAssigned() {
    // TODO: we have a better way now (wait on event)
    return masterServices.getAssignmentManager().getRegionStates()
      .hasTableRegionStates(TableName.NAMESPACE_TABLE_NAME);
  }

  public void validateTableAndRegionCount(NamespaceDescriptor desc) throws IOException {
    if (getMaxRegions(desc) <= 0) {
      throw new ConstraintException(
        "The max region quota for " + desc.getName() + " is less than or equal to zero.");
    }
    if (getMaxTables(desc) <= 0) {
      throw new ConstraintException(
        "The max tables quota for " + desc.getName() + " is less than or equal to zero.");
    }
  }

  public static long getMaxTables(NamespaceDescriptor ns) throws IOException {
    String value = ns.getConfigurationValue(KEY_MAX_TABLES);
    long maxTables = 0;
    if (StringUtils.isNotEmpty(value)) {
      try {
        maxTables = Long.parseLong(value);
      } catch (NumberFormatException exp) {
        throw new DoNotRetryIOException("NumberFormatException while getting max tables.", exp);
      }
    } else {
      // The property is not set, so assume its the max long value.
      maxTables = Long.MAX_VALUE;
    }
    return maxTables;
  }

  public static long getMaxRegions(NamespaceDescriptor ns) throws IOException {
    String value = ns.getConfigurationValue(KEY_MAX_REGIONS);
    long maxRegions = 0;
    if (StringUtils.isNotEmpty(value)) {
      try {
        maxRegions = Long.parseLong(value);
      } catch (NumberFormatException exp) {
        throw new DoNotRetryIOException("NumberFormatException while getting max regions.", exp);
      }
    } else {
      // The property is not set, so assume its the max long value.
      maxRegions = Long.MAX_VALUE;
    }
    return maxRegions;
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  @Override
  public void stop(String why) {
    if (this.stopped) {
      return;
    }
    try {
      if (this.zkNamespaceManager != null) {
        this.zkNamespaceManager.stop();
      }
    } catch (IOException ioe) {
      LOG.warn("Failed NamespaceManager close", ioe);
    }
    try {
      if (this.nsTable != null) {
        this.nsTable.close();
      }
    } catch (IOException ioe) {
      LOG.warn("Failed Namespace Table close", ioe);
    }
    this.stopped = true;
  }
}
