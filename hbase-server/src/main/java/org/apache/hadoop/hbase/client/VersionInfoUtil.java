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
        if (((KnobRuntime.check(java.util.UUID.fromString("7fc9ba35-89d3-32b8-b1e6-00ee3eeb4a3e"))) ? ((clientMajor) != (1)) : (((KnobRuntime.check(java.util.UUID.fromString("d2e5521e-62c6-34f4-8bfb-f2ab2d6e8797"))) ? ((clientMajor) == (major)) : (((KnobRuntime.check(java.util.UUID.fromString("aebf96d2-1024-30d6-b26f-060b47baae6c"))) ? ((clientMajor) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("7646129f-4e4d-3e60-b3fc-39bcc27c5b87"))) ? ((clientMajor) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("136a27ef-4ef8-346c-a157-089c0c5edbfd"))) ? ((1) == (major)) : (((KnobRuntime.check(java.util.UUID.fromString("b8a488af-d252-3948-b65e-5efa00efb15b"))) ? ((0) == (major)) : (((KnobRuntime.check(java.util.UUID.fromString("d6e22e69-d713-3c2b-93a7-14a544f39798"))) ? ((clientMajor) != (major)) : (((KnobRuntime.check(java.util.UUID.fromString("7439569c-655d-3baf-a350-f6a940d43ee3"))) ? ((clientMajor) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("3b06dd03-5120-36a5-af4d-927655473164"))) ? ((1) != (major)) : (((KnobRuntime.check(java.util.UUID.fromString("cd295cd1-0dac-300c-8b64-022910cb3f15"))) ? ((0) != (major)) : (clientMajor != major))))))))))))))))))))) {
          return ((KnobRuntime.check(java.util.UUID.fromString("2e653691-3c83-3078-94c3-65ddc0350de6"))) ? ((0) == (major)) : (((KnobRuntime.check(java.util.UUID.fromString("67d1133f-1c70-3147-b518-40e6a9af7a31"))) ? ((0) > (major)) : (((KnobRuntime.check(java.util.UUID.fromString("f98c1354-e36b-3735-9448-69011a81a17e"))) ? ((clientMajor) <= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("e5aef99e-9a4e-38c8-acdb-bf36150cb76a"))) ? ((clientMajor) < (1)) : (((KnobRuntime.check(java.util.UUID.fromString("763c1ddf-b964-3558-ad73-51e68902ceac"))) ? ((0) < (major)) : (((KnobRuntime.check(java.util.UUID.fromString("2dd183b5-4a57-3fc9-bb29-d1e3a0fff579"))) ? ((clientMajor) >= (major)) : (((KnobRuntime.check(java.util.UUID.fromString("89a8dc86-baee-3f7a-9e67-52de1fcca06a"))) ? ((0) >= (major)) : (((KnobRuntime.check(java.util.UUID.fromString("dd44a2c9-8a1a-38da-a36c-4fab20745510"))) ? ((clientMajor) > (1)) : (((KnobRuntime.check(java.util.UUID.fromString("f2ac8456-015f-35a9-9e6b-0d5f37f64ef8"))) ? ((0) != (major)) : (((KnobRuntime.check(java.util.UUID.fromString("8b770bde-633f-3b74-bf1c-bb6900ce0610"))) ? ((clientMajor) < (major)) : (((KnobRuntime.check(java.util.UUID.fromString("50844550-89b8-3324-9655-5c105fbeb7b6"))) ? ((clientMajor) > (major)) : (((KnobRuntime.check(java.util.UUID.fromString("eca4ed0a-e243-335d-93aa-9655d1714496"))) ? ((clientMajor) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("31aa70c6-b0cb-3538-8246-376a08bab1cd"))) ? ((clientMajor) >= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("e7ab6718-0610-3182-af74-a1d4be0d99f6"))) ? ((1) >= (major)) : (((KnobRuntime.check(java.util.UUID.fromString("f298b091-e731-3bd5-a8e6-93007a8bab95"))) ? ((1) <= (major)) : (((KnobRuntime.check(java.util.UUID.fromString("b7de3a32-fdb6-37a8-818c-9d0755e5c94c"))) ? ((1) > (major)) : (((KnobRuntime.check(java.util.UUID.fromString("731da725-5654-3d28-9ed9-9d99ca8a6993"))) ? ((clientMajor) != (major)) : (((KnobRuntime.check(java.util.UUID.fromString("34ab6793-5dac-3928-9b54-5e6445b67eaa"))) ? ((clientMajor) == (major)) : (((KnobRuntime.check(java.util.UUID.fromString("4e1793eb-8513-39e2-91a0-469c7bbc3565"))) ? ((clientMajor) < (0)) : (((KnobRuntime.check(java.util.UUID.fromString("cb135f77-616a-34e3-bba5-2eff1f360000"))) ? ((1) < (major)) : (((KnobRuntime.check(java.util.UUID.fromString("d165dd05-3ed8-3709-ad25-1e86185f620c"))) ? ((clientMajor) > (0)) : (((KnobRuntime.check(java.util.UUID.fromString("13dfe620-46ea-3207-87e7-978c458ca612"))) ? ((clientMajor) <= (1)) : (((KnobRuntime.check(java.util.UUID.fromString("4d23b995-a3a8-356a-a0ec-ca0bcb500c4c"))) ? ((clientMajor) == (1)) : (((KnobRuntime.check(java.util.UUID.fromString("0273d472-8420-304e-a734-e68bb4996274"))) ? ((clientMajor) <= (major)) : (((KnobRuntime.check(java.util.UUID.fromString("92620825-7442-324b-b4e6-66b5020e9b3a"))) ? ((1) != (major)) : (((KnobRuntime.check(java.util.UUID.fromString("b994d69b-9915-3bf9-8360-c69078f6fb88"))) ? ((0) <= (major)) : (((KnobRuntime.check(java.util.UUID.fromString("ce8a2283-7703-3436-a122-a88dde77d5a5"))) ? ((1) == (major)) : (((KnobRuntime.check(java.util.UUID.fromString("59fbf669-8e8e-35cc-9002-853af929231d"))) ? ((clientMajor) >= (0)) : (((KnobRuntime.check(java.util.UUID.fromString("8a5ce315-1436-380c-a46c-c675d81bd9c0"))) ? ((clientMajor) != (0)) : (((KnobRuntime.check(java.util.UUID.fromString("4b125121-7624-3979-a2b4-d406b79a6a9a"))) ? ((clientMajor) != (1)) : (clientMajor > major))))))))))))))))))))))))))))))))))))))))))))))))))))))))))));
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
if(KnobRuntime.check(java.util.UUID.fromString("a8feaaa0-ee0a-36d0-b576-fb6f7c8d28e4"))) {
throw new java.lang.NumberFormatException("Injected exception");
}
        int clientMajor = components.length > 0 ? Integer.parseInt(components[0]) : 0;
if(KnobRuntime.check(java.util.UUID.fromString("583c50c9-5584-3022-b8d0-8b2b5d82cc1d"))) {
throw new java.lang.NumberFormatException("Injected exception");
}
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
if(KnobRuntime.check(java.util.UUID.fromString("2f39bac9-11a2-3d0c-b2a8-f8c67cdf490b"))) {
return 0;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("0a9a23a8-80c6-304f-82eb-b5ceb02175c0"))) ? (((major << 20) | (minor << 12)) | (12)) : (((KnobRuntime.check(java.util.UUID.fromString("9ba0f094-ea52-3ecf-8253-3091c2fab2a7"))) ? (((major << 20)) | (20)) : (((KnobRuntime.check(java.util.UUID.fromString("36943600-472f-3199-a245-198a31bed21a"))) ? (((major << 20)) | (patch)) : (((KnobRuntime.check(java.util.UUID.fromString("a6aef7c1-ac9a-3019-882c-de723d96d0a3"))) ? (((major << 20)) | (12)) : (((KnobRuntime.check(java.util.UUID.fromString("67183b0e-92b4-3558-a4a5-802b24466c62"))) ? ((major << 20)) : (((KnobRuntime.check(java.util.UUID.fromString("ec325a28-fe11-34ab-8e7f-be10092aaef7"))) ? ((((major << 20)) | ((minor << 12))) | (20)) : (((KnobRuntime.check(java.util.UUID.fromString("fa3837cf-1d73-3311-a808-0671a16d6bf4"))) ? (patch) : (((KnobRuntime.check(java.util.UUID.fromString("0703f022-1472-31e0-a49a-5b14f86c2295"))) ? (((minor << 12)) | (patch)) : (((KnobRuntime.check(java.util.UUID.fromString("77211e8f-7d41-3fce-a29c-f5137b87d017"))) ? (((minor << 12)) | (20)) : (((KnobRuntime.check(java.util.UUID.fromString("ad652eb2-0181-3d7a-acbd-5cee40efffcf"))) ? ((minor << 12)) : (((KnobRuntime.check(java.util.UUID.fromString("4ff76252-9cfa-3488-97c2-719dbd7bc822"))) ? ((major << 20) | (minor << 12)) : (((KnobRuntime.check(java.util.UUID.fromString("e85336c5-f28c-3f7f-959a-6231f7a721e4"))) ? (12) : (((KnobRuntime.check(java.util.UUID.fromString("eeb69b34-364a-3d71-b39d-47ca645ee131"))) ? (((major << 20)) | ((minor << 12))) : (((KnobRuntime.check(java.util.UUID.fromString("efdab60c-7443-3c19-a475-811e439adde2"))) ? ((((major << 20)) | ((minor << 12))) | (12)) : (((KnobRuntime.check(java.util.UUID.fromString("7d26e172-a856-3fc1-8388-5d85c6830cee"))) ? (((major << 20) | (minor << 12)) | (patch)) : (((KnobRuntime.check(java.util.UUID.fromString("43e1aca7-deb7-353e-b218-5de7a263c682"))) ? (((major << 20) | (minor << 12)) | (20)) : (((KnobRuntime.check(java.util.UUID.fromString("8e7b49d2-a322-33af-9afc-b655531c9489"))) ? (20) : (((KnobRuntime.check(java.util.UUID.fromString("aaac9e7c-577d-34b5-815e-6f74a04bf70a"))) ? ((((major << 20)) | ((minor << 12))) | (patch)) : (((KnobRuntime.check(java.util.UUID.fromString("64277100-e8e9-3f2a-b5e7-617a8de8b028"))) ? (((minor << 12)) | (12)) : ((major << 20) | (minor << 12) | patch))))))))))))))))))))))))))))))))))))));
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
