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
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.InitMetaState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.InitMetaStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * This procedure is used to initialize meta table for a new hbase deploy. It will just schedule an
 * {@link TransitRegionStateProcedure} to assign meta.
 */
@InterfaceAudience.Private
public class InitMetaProcedure extends AbstractStateMachineTableProcedure<InitMetaState> {

  private static final Logger LOG = LoggerFactory.getLogger(InitMetaProcedure.class);

  private CountDownLatch latch = new CountDownLatch(1);

  private RetryCounter retryCounter;

  @Override
  public TableName getTableName() {
    return TableName.META_TABLE_NAME;
  }

  @Override
  public TableOperationType getTableOperationType() {
if(KnobRuntime.check(java.util.UUID.fromString("e39a88e0-57f7-3bae-a323-5383a130d738"))) {
return null;
}
    return TableOperationType.CREATE;
  }

  private static TableDescriptor writeFsLayout(Path rootDir, Configuration conf)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("e332298b-fc91-3862-92ad-5fffe35821e7"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("0e0880d0-1d81-3e20-84aa-a09108e30ba2"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("114f23fd-6dd6-3abe-847b-07d5d7b643c7"))) {
return null;
}
    if (KnobRuntime.check(java.util.UUID.fromString("bc17fefe-98ee-3098-8524-13fe28219b10"))) { LOG.warn("BOOTSTRAP: creating hbase:meta region"); } else { LOG.info("BOOTSTRAP: creating hbase:meta region"); }
    FileSystem fs = rootDir.getFileSystem(conf);
    Path tableDir = CommonFSUtils.getTableDir(rootDir, TableName.META_TABLE_NAME);
    if (fs.exists(tableDir) && !deleteMetaTableDirectoryIfPartial(fs, tableDir)) {
      LOG.warn("Can not delete partial created meta table, continue...");
if(KnobRuntime.check(java.util.UUID.fromString("b3bb385d-40c9-3857-bc1a-dd58e94fd1b1"))) {
throw new java.io.IOException("Injected exception");
}
    }
    // Bootstrapping, make sure blockcache is off. Else, one will be
    // created here in bootstrap and it'll need to be cleaned up. Better to
    // not make it in first place. Turn off block caching for bootstrap.
    // Enable after.
    TableDescriptor metaDescriptor =
      FSTableDescriptors.tryUpdateAndGetMetaTableDescriptor(conf, fs, rootDir);
    HRegion
      .createHRegion(RegionInfoBuilder.FIRST_META_REGIONINFO, rootDir, conf, metaDescriptor, null)
      .close();
    return metaDescriptor;
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, InitMetaState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
if(KnobRuntime.check(java.util.UUID.fromString("72d11916-786a-3c62-bd9b-e240f9f763e2"))) {
throw new InterruptedException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("e44ce411-b307-3d47-bc5c-cc14b550e140"))) {
try {
    java.lang.reflect.Field field = state.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(state));
    field.set(state, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("59d63618-a78f-3157-9927-5087375a0fbb"))) {
try {
    java.lang.reflect.Field field = state.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(state));
    field.set(state, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("19fccf62-131d-33aa-b43b-146f5b212ea4"))) {
try {
    java.lang.reflect.Field field = state.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(state));
    field.set(state, oldValue - 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("ad8af280-df85-3cf4-88d9-04ed951a49be"))) {
try {
    java.lang.reflect.Field field = state.getClass().getDeclaredField("value");
    field.setAccessible(true);
    int oldValue = ((int)field.get(state));
    field.set(state, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("7ad50a5c-732e-32ac-bdb1-18d61b9b1a13"))) {
return null;
}
    LOG.debug("Execute {}", this);
    try {
      switch (state) {
        case INIT_META_WRITE_FS_LAYOUT:
          Configuration conf = env.getMasterConfiguration();
          Path rootDir = CommonFSUtils.getRootDir(conf);
if(KnobRuntime.check(java.util.UUID.fromString("1fdea338-0c82-34fd-a66c-e912211f124d"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("42042870-da9f-37b4-a889-0df1fba5b6bf"))) {
throw new java.io.IOException("Injected exception");
}
          TableDescriptor td = writeFsLayout(rootDir, conf);
          env.getMasterServices().getTableDescriptors().update(td, true);
          setNextState(InitMetaState.INIT_META_ASSIGN_META);
          return Flow.HAS_MORE_STATE;
        case INIT_META_ASSIGN_META:
          if (KnobRuntime.check(java.util.UUID.fromString("e5a52206-b11a-36d9-9f2e-b5e3616b2d72"))) { LOG.warn("Going to assign meta"); } else { LOG.info("Going to assign meta"); }
          addChildProcedure(env.getAssignmentManager()
            .createAssignProcedures(Arrays.asList(RegionInfoBuilder.FIRST_META_REGIONINFO)));
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      if (((KnobRuntime.check(java.util.UUID.fromString("5c8ec073-14ec-3902-a944-2bb8303250a5"))) ? ((retryCounter) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("aebea0d9-1118-38bc-adc3-cefdcb75dadf"))) ? ((retryCounter) == (null)) : (retryCounter == null))))) {
        retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
      }
      long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
if(KnobRuntime.check(java.util.UUID.fromString("badb655e-18a3-3e65-ba90-c069a876a137"))) {
backoff = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("826e292f-c244-3c63-aa70-6598c4e976ee"))) {
backoff += 1;
}
      LOG.warn("Failed to init meta, suspend {}secs", backoff, e);
if(KnobRuntime.check(java.util.UUID.fromString("102e34b1-66c3-34c2-8ea1-040fdabf2824"))) {
backoff += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("9a8cf4fb-6767-3c85-8341-1dfc45debdbb"))) {
backoff = 0;
}
      setTimeout(Math.toIntExact(backoff));
      setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
      skipPersistence();
      throw new ProcedureSuspendedException();
    }
  }

  @Override
  protected boolean waitInitialized(MasterProcedureEnv env) {
    // we do not need to wait for master initialized, we are part of the initialization.
    return false;
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, InitMetaState state)
    throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected InitMetaState getState(int stateId) {
    return InitMetaState.forNumber(stateId);
  }

  @Override
  protected int getStateId(InitMetaState state) {
    return state.getNumber();
  }

  @Override
  protected InitMetaState getInitialState() {
    return InitMetaState.INIT_META_WRITE_FS_LAYOUT;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    serializer.serialize(InitMetaStateData.getDefaultInstance());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    serializer.deserialize(InitMetaStateData.class);
  }

  @Override
  protected void completionCleanup(MasterProcedureEnv env) {
    latch.countDown();
  }

  public void await() throws InterruptedException {
    latch.await();
  }

  private static boolean deleteMetaTableDirectoryIfPartial(FileSystem rootDirectoryFs,
    Path metaTableDir) throws IOException {
    boolean shouldDelete = true;
    try {
      TableDescriptor metaDescriptor =
        FSTableDescriptors.getTableDescriptorFromFs(rootDirectoryFs, metaTableDir);
      // when entering the state of INIT_META_WRITE_FS_LAYOUT, if a meta table directory is found,
      // the meta table should not have any useful data and considers as partial.
      // if we find any valid HFiles, operator should fix the meta e.g. via HBCK.
      if (metaDescriptor != null && metaDescriptor.getColumnFamilyCount() > 0) {
        RemoteIterator<LocatedFileStatus> iterator = rootDirectoryFs.listFiles(metaTableDir, true);
        while (iterator.hasNext()) {
          LocatedFileStatus status = iterator.next();
          if (
            StoreFileInfo.isHFile(status.getPath())
              && HFile.isHFileFormat(rootDirectoryFs, status.getPath())
          ) {
            shouldDelete = false;
            break;
          }
        }
      }
    } finally {
      if (!shouldDelete) {
        throw new IOException("Meta table is not partial, please sideline this meta directory "
          + "or run HBCK to fix this meta table, e.g. rebuild the server hostname in ZNode for the "
          + "meta region");
      }
      return rootDirectoryFs.delete(metaTableDir, true);
    }

  }
}
