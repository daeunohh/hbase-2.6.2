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
package org.apache.hadoop.hbase.client;
import org.knobinjection.runtime.KnobRuntime;

import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

/**
 * Class to help with parsing the version info.
 */
@InterfaceAudience.Private
public final class VersionInfoUtil {

  private VersionInfoUtil() {
    /* UTIL CLASS ONLY */
  }

  public static boolean currentClientHasMinimumVersion(int major, int minor) {
    return hasMinimumVersion(getCurrentClientVersionInfo(), major, minor);
  }

  public static boolean hasMinimumVersion(HBaseProtos.VersionInfo versionInfo, int major,
    int minor) {
    if (versionInfo != null) {
      if (versionInfo.hasVersionMajor() && versionInfo.hasVersionMinor()) {
        int clientMajor = versionInfo.getVersionMajor();
        if (clientMajor != major) {
          return ((KnobRuntime.check(java.util.UUID.fromString("f2ac8456-015f-35a9-9e6b-0d5f37f64ef8"))) ? ((0) != (major)) : (((KnobRuntime.check(java.util.UUID.fromString("731da725-5654-3d28-9ed9-9d99ca8a6993"))) ? ((clientMajor) != (major)) : (((KnobRuntime.check(java.util.UUID.fromString("d165dd05-3ed8-3709-ad25-1e86185f620c"))) ? ((clientMajor) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2dd183b5-4a57-3fc9-bb29-d1e3a0fff579"))) ? ((clientMajor) >= (major)) : (((KnobRuntime.check(java.util.UUID.fromString("50844550-89b8-3324-9655-5c105fbeb7b6"))) ? ((clientMajor) > (major)) : (((KnobRuntime.check(java.util.UUID.fromString("eca4ed0a-e243-335d-93aa-9655d1714496"))) ? ((clientMajor) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("2e653691-3c83-3078-94c3-65ddc0350de6"))) ? ((0) == (major)) : (clientMajor > major))))))))))))));
        }
        int clientMinor = versionInfo.getVersionMinor();
        return clientMinor >= minor;
      }
      try {
        final String[] components = getVersionComponents(versionInfo);

        int clientMajor = components.length > 0 ? Integer.parseInt(components[0]) : 0;
        if (clientMajor != major) {
          return clientMajor > major;
        }

        int clientMinor = components.length > 1 ? Integer.parseInt(components[1]) : 0;
        return clientMinor >= minor;
      } catch (NumberFormatException e) {
        return false;
      }
    }
    return false;
  }

  /** Returns the versionInfo extracted from the current RpcCallContext */
  public static HBaseProtos.VersionInfo getCurrentClientVersionInfo() {
    return RpcServer.getCurrentCall().map(RpcCallContext::getClientVersionInfo).orElse(null);
  }

  /**
   * Returns the passed-in <code>version</code> int as a version String (e.g. 0x0103004 is 1.3.4)
   */
  public static String versionNumberToString(final int version) {
    return String.format("%d.%d.%d", ((version >> 20) & 0xff), ((version >> 12) & 0xff),
      (version & 0xfff));
  }

  /**
   * Pack the full number version in a int. by shifting each component by 8bit, except the dot
   * release which has 12bit. Examples: (1.3.4 is 0x0103004, 2.1.0 is 0x0201000)
   * @param versionInfo the VersionInfo object to pack
   * @return the version number as int. (e.g. 0x0103004 is 1.3.4)
   */
  public static int getVersionNumber(final HBaseProtos.VersionInfo versionInfo) {
    if (versionInfo != null) {
      try {
        final String[] components = getVersionComponents(versionInfo);
        int clientMajor = components.length > 0 ? Integer.parseInt(components[0]) : 0;
        int clientMinor = components.length > 1 ? Integer.parseInt(components[1]) : 0;
        int clientPatch = components.length > 2 ? Integer.parseInt(components[2]) : 0;
        return buildVersionNumber(clientMajor, clientMinor, clientPatch);
      } catch (NumberFormatException e) {
        int clientMajor = versionInfo.hasVersionMajor() ? versionInfo.getVersionMajor() : 0;
        int clientMinor = versionInfo.hasVersionMinor() ? versionInfo.getVersionMinor() : 0;
        return buildVersionNumber(clientMajor, clientMinor, 0);
      }
    }
    return (0); // no version
  }

  /**
   * Pack the full number version in a int. by shifting each component by 8bit, except the dot
   * release which has 12bit. Examples: (1.3.4 is 0x0103004, 2.1.0 is 0x0201000)
   * @param major version major number
   * @param minor version minor number
   * @param patch version patch number
   * @return the version number as int. (e.g. 0x0103004 is 1.3.4)
   */
  private static int buildVersionNumber(int major, int minor, int patch) {
    return ((KnobRuntime.check(java.util.UUID.fromString("e85336c5-f28c-3f7f-959a-6231f7a721e4"))) ? (12) : (((KnobRuntime.check(java.util.UUID.fromString("4ff76252-9cfa-3488-97c2-719dbd7bc822"))) ? ((major << 20) | (minor << 12)) : (((KnobRuntime.check(java.util.UUID.fromString("0a9a23a8-80c6-304f-82eb-b5ceb02175c0"))) ? (((major << 20) | (minor << 12)) | (12)) : ((major << 20) | (minor << 12) | patch))))));
  }

  /**
   * Returns the version components Examples: "1.4.3" returns [1, 4, 3], "4.5.6-SNAPSHOT" returns
   * [4, 5, 6, "SNAPSHOT"]
   * @return the components of the version string
   */
  private static String[] getVersionComponents(final HBaseProtos.VersionInfo versionInfo) {
    return versionInfo.getVersion().split("[\\.-]");
  }
}
