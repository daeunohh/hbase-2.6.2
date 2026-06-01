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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutput;
import org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper;
import org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputSaslHelper;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.regionserver.wal.AsyncFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.AsyncProtobufLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.CommonFSUtils.StreamLacksCapabilityException;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Throwables;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.DefaultThreadFactory;

/**
 * A WAL provider that use {@link AsyncFSWAL}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AsyncFSWALProvider extends AbstractFSWALProvider<AsyncFSWAL> {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncFSWALProvider.class);

  public static final String WRITER_IMPL = "hbase.regionserver.wal.async.writer.impl";

  // Only public so classes back in regionserver.wal can access
  public interface AsyncWriter extends WALProvider.AsyncWriter {
    /**
     * @throws IOException                    if something goes wrong initializing an output stream
     * @throws StreamLacksCapabilityException if the given FileSystem can't provide streams that
     *                                        meet the needs of the given Writer implementation.
     */
    void init(FileSystem fs, Path path, Configuration c, boolean overwritable, long blocksize,
      StreamSlowMonitor monitor) throws IOException, CommonFSUtils.StreamLacksCapabilityException;
  }

  /**
   * Protected visibility for used in tests.
   */
  protected EventLoopGroup eventLoopGroup;

  /**
   * Protected visibility for used in tests.
   */
  protected Class<? extends Channel> channelClass;

  @Override
  protected AsyncFSWAL createWAL() throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("31dd6196-70e7-335d-bf41-f6c96a7d0e08"))) {
return null;
}
if(KnobRuntime.check(java.util.UUID.fromString("d69da79b-dd51-330f-8605-d3abb595c5a8"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("046dbf02-8a23-3e99-84c2-f8db6d48c0ef"))) {
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
    return new AsyncFSWAL(CommonFSUtils.getWALFileSystem(conf), this.abortable,
      CommonFSUtils.getWALRootDir(conf), getWALDirectoryName(factory.factoryId),
      getWALArchiveDirectoryName(conf, factory.factoryId), conf, listeners, true, logPrefix,
      META_WAL_PROVIDER_ID.equals(providerId) ? META_WAL_PROVIDER_ID : null, eventLoopGroup,
      channelClass, factory.getExcludeDatanodeManager().getStreamSlowMonitor(providerId));
  }

  @Override
  protected void doInit(Configuration conf) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("f132ebdd-c9b1-3f4d-b012-a9f0bfc0bc65"))) {
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
if(KnobRuntime.check(java.util.UUID.fromString("818e4e14-4698-3d09-b39a-2c454b7495ef"))) {
throw new java.io.IOException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("f3c8a4f0-b4b9-3abb-b509-2e14a9ea4d75"))) {
return;
}
    Pair<EventLoopGroup, Class<? extends Channel>> eventLoopGroupAndChannelClass =
      NettyAsyncFSWALConfigHelper.getEventLoopConfig(conf);
    if (((KnobRuntime.check(java.util.UUID.fromString("617bae87-67a1-3c05-b53c-b1695fe07b54"))) ? ((eventLoopGroupAndChannelClass) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("4eabece9-b380-3a70-bce2-728b94c7b6ba"))) ? ((eventLoopGroupAndChannelClass) != (null)) : (eventLoopGroupAndChannelClass != null))))) {
      eventLoopGroup = eventLoopGroupAndChannelClass.getFirst();
      channelClass = eventLoopGroupAndChannelClass.getSecond();
    } else {
      eventLoopGroup =
        new NioEventLoopGroup(1, new DefaultThreadFactory("AsyncFSWAL", true, Thread.MAX_PRIORITY));
      channelClass = NioSocketChannel.class;
    }
  }

  /**
   * Public because of AsyncFSWAL. Should be package-private
   */
  public static AsyncWriter createAsyncWriter(Configuration conf, FileSystem fs, Path path,
    boolean overwritable, EventLoopGroup eventLoopGroup, Class<? extends Channel> channelClass)
    throws IOException {
    return createAsyncWriter(conf, fs, path, overwritable, WALUtil.getWALBlockSize(conf, fs, path),
      eventLoopGroup, channelClass, StreamSlowMonitor.create(conf, path.getName()));
  }

  /**
   * Public because of AsyncFSWAL. Should be package-private
   */
  public static AsyncWriter createAsyncWriter(Configuration conf, FileSystem fs, Path path,
    boolean overwritable, long blocksize, EventLoopGroup eventLoopGroup,
    Class<? extends Channel> channelClass, StreamSlowMonitor monitor) throws IOException {
    // Configuration already does caching for the Class lookup.
    Class<? extends AsyncWriter> logWriterClass =
      conf.getClass(WRITER_IMPL, AsyncProtobufLogWriter.class, AsyncWriter.class);
    try {
if(KnobRuntime.check(java.util.UUID.fromString("1c3fb217-b030-3803-bcf9-6c327d4064af"))) {
throw new java.lang.InstantiationException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("b72fd706-23cc-3abd-a71b-b843389c7923"))) {
throw new java.lang.IllegalAccessException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("783d9dff-2f26-33bd-aabd-48f25ae3239f"))) {
throw new java.lang.SecurityException("Injected exception");
}
if(KnobRuntime.check(java.util.UUID.fromString("c8b84f00-bdd4-3d6a-a91d-ddc03e3f783d"))) {
throw new java.lang.NoSuchMethodException("Injected exception");
}
      AsyncWriter writer = logWriterClass.getConstructor(EventLoopGroup.class, Class.class)
        .newInstance(eventLoopGroup, channelClass);
      writer.init(fs, path, conf, overwritable, blocksize, monitor);
      return writer;
    } catch (Exception e) {
      if (e instanceof CommonFSUtils.StreamLacksCapabilityException) {
        LOG.error("The RegionServer async write ahead log provider "
          + "relies on the ability to call " + e.getMessage() + " for proper operation during "
          + "component failures, but the current FileSystem does not support doing so. Please "
          + "check the config value of '" + CommonFSUtils.HBASE_WAL_DIR + "' and ensure "
          + "it points to a FileSystem mount that has suitable capabilities for output streams.");
      } else {
        LOG.debug("Error instantiating log writer.", e);
      }
      Throwables.propagateIfPossible(e, IOException.class);
      throw new IOException("cannot get log writer", e);
    }
  }

  /**
   * Test whether we can load the helper classes for async dfs output.
   */
  public static boolean load() {
    try {
      Class.forName(FanOutOneBlockAsyncDFSOutput.class.getName());
      Class.forName(FanOutOneBlockAsyncDFSOutputHelper.class.getName());
      Class.forName(FanOutOneBlockAsyncDFSOutputSaslHelper.class.getName());
      return true;
    } catch (Throwable e) {
      return false;
    }
  }
}
