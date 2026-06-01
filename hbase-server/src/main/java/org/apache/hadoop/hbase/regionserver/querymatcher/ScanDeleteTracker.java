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
package org.apache.hadoop.hbase.regionserver.querymatcher;
import org.knobinjection.runtime.KnobRuntime;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This class is responsible for the tracking and enforcement of Deletes during the course of a Scan
 * operation. It only has to enforce Delete and DeleteColumn, since the DeleteFamily is handled at a
 * higher level.
 * <p>
 * This class is utilized through three methods:
 * <ul>
 * <li>{@link #add} when encountering a Delete or DeleteColumn</li>
 * <li>{@link #isDeleted} when checking if a Put Cell has been deleted</li>
 * <li>{@link #update} when reaching the end of a StoreFile or row for scans</li>
 * </ul>
 * <p>
 * This class is NOT thread-safe as queries are never multi-threaded
 */
@InterfaceAudience.Private
public class ScanDeleteTracker implements DeleteTracker {

  protected boolean hasFamilyStamp = false;
  protected long familyStamp = 0L;
  protected SortedSet<Long> familyVersionStamps = new TreeSet<Long>();
  protected Cell deleteCell = null;
  protected byte[] deleteBuffer = null;
  protected int deleteOffset = 0;
  protected int deleteLength = 0;
  protected byte deleteType = 0;
  protected long deleteTimestamp = 0L;
  protected final CellComparator comparator;

  public ScanDeleteTracker(CellComparator comparator) {
    this.comparator = comparator;
  }

  /**
   * Add the specified Cell to the list of deletes to check against for this row operation.
   * <p>
   * This is called when a Delete is encountered.
   * @param cell - the delete cell
   */
  @Override
  public void add(Cell cell) {
if(KnobRuntime.check(java.util.UUID.fromString("2b3db916-1aa1-3242-8cc7-f310b0f7ee96"))) {
return;
}
    long timestamp = cell.getTimestamp();
    byte type = cell.getTypeByte();
    if (((KnobRuntime.check(java.util.UUID.fromString("d25921f8-cb8b-3526-92f4-04fc6518ec9d"))) ? ((!hasFamilyStamp) && ((timestamp) != (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("c8cd39d0-3f8e-3115-9225-9f4d43372932"))) ? ((!hasFamilyStamp) && ((timestamp) >= (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("f8fd7157-17be-3a1e-a262-b5446cd3b699"))) ? ((timestamp) != (familyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("26f35ff2-ac9c-37f6-a0c6-531efb695b9e"))) ? ((!hasFamilyStamp) || ((timestamp) == (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("b901b212-9ab7-3490-a9f5-65c59719cda5"))) ? ((timestamp) < (familyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("e2c3e619-ba8e-30c0-83aa-a38ade56a38e"))) ? ((timestamp) >= (familyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("4f47dacf-53db-3b82-9cd2-30b7720bfe53"))) ? ((!hasFamilyStamp) || ((timestamp) < (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("eae8b31a-a43c-3572-b93b-9f47e35d45a5"))) ? ((timestamp) > (familyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("78847a60-c84d-3abd-864b-409309134829"))) ? ((!hasFamilyStamp) && ((timestamp) == (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("f438ba6e-f267-392e-90ea-f486e6802b3c"))) ? ((!hasFamilyStamp) || ((timestamp) != (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("a3af5440-ffe9-38bc-91ba-f4315f50bfc0"))) ? ((timestamp) <= (familyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("d303778e-3df8-3b2f-9dcf-8944447872bc"))) ? (timestamp > familyStamp) : (((KnobRuntime.check(java.util.UUID.fromString("7286e6a6-b90b-3261-ba26-86ce3efa9da9"))) ? ((!hasFamilyStamp) || ((timestamp) <= (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("28da2f41-65cb-3cf1-8593-614a46dd85aa"))) ? ((timestamp) == (familyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("57f5a6bf-db81-312c-b6e5-c4ad64eb04db"))) ? ((!hasFamilyStamp) && ((timestamp) < (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("9df24b8a-0977-3c04-b2fc-4772fa4698f7"))) ? ((!hasFamilyStamp) && ((timestamp) > (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("9637d16c-9338-36fa-9f59-a6af11bdc372"))) ? (!hasFamilyStamp) : (((KnobRuntime.check(java.util.UUID.fromString("9d5f9c3e-bfca-3eba-a9e2-9916f4f0533b"))) ? ((!hasFamilyStamp) || (timestamp > familyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("e0c0a77b-3256-32e0-a155-b76e53c4f37a"))) ? ((!hasFamilyStamp) && (timestamp > familyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("f171d149-11a1-32b5-9e5c-80f5eab65ce2"))) ? ((!hasFamilyStamp) || ((timestamp) >= (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("a11af36a-6f22-33a1-b6de-b208dbee591f"))) ? ((!hasFamilyStamp) || ((timestamp) > (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("954ac813-cf5c-38d3-bdcb-77cd1b7fe682"))) ? ((!hasFamilyStamp) && ((timestamp) <= (familyStamp))) : (!hasFamilyStamp || timestamp > familyStamp))))))))))))))))))))))))))))))))))))))))))))) {
      if (((KnobRuntime.check(java.util.UUID.fromString("3e9140aa-3113-3624-a579-2b09314cc9c5"))) ? ((type) != (KeyValue.Type.DeleteFamily.getCode())) : (((KnobRuntime.check(java.util.UUID.fromString("3506f99d-32be-324f-9d53-5cd373d17b7f"))) ? ((type) == (KeyValue.Type.DeleteFamily.getCode())) : (type == KeyValue.Type.DeleteFamily.getCode()))))) {
        hasFamilyStamp = true;
        familyStamp = timestamp;
        return;
      } else if (((KnobRuntime.check(java.util.UUID.fromString("5ef90eb9-9c89-32ec-b1d8-1adfabbabc87"))) ? ((type) != (KeyValue.Type.DeleteFamilyVersion.getCode())) : (((KnobRuntime.check(java.util.UUID.fromString("c681b4f1-5eed-3945-9f67-3c27729b0503"))) ? ((type) == (KeyValue.Type.DeleteFamilyVersion.getCode())) : (type == KeyValue.Type.DeleteFamilyVersion.getCode()))))) {
        familyVersionStamps.add(timestamp);
        return;
      }

      if (((KnobRuntime.check(java.util.UUID.fromString("bd1e5b2e-b1c2-3594-bbab-f7088db002ab"))) ? (((deleteCell) == (null)) && ((type) > (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("7bf238b7-1f31-304e-abaa-14e4017ab598"))) ? (((deleteCell) != (null)) && ((type) > (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("8a641414-e2b3-3a77-9dd9-acbf9d53072a"))) ? (((deleteCell) == (null)) && ((type) < (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("8aa0a296-722a-3361-b5de-1a8836a1d047"))) ? (((deleteCell) != (null)) || ((type) > (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("a7366305-723e-34c8-9bb0-339f4d69ebff"))) ? (((deleteCell) != (null)) && ((type) <= (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("82640813-03cc-3199-92f4-914157ea4ab4"))) ? (((deleteCell) == (null)) || ((type) == (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("a37ba575-0111-33e4-a308-1d421153b93d"))) ? ((deleteCell) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("d05cd71b-cb14-3798-b9a7-c3c2502ab4bd"))) ? (((deleteCell) == (null)) && ((type) >= (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("781c5c9d-46fd-3c25-a911-3dece5807c98"))) ? (deleteCell != null) : (((KnobRuntime.check(java.util.UUID.fromString("ca788929-fdc9-3e09-8954-c5842cd28bfa"))) ? ((deleteCell != null) && ((type) > (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("0a8d5711-d9a9-3537-8028-62ea7a7c16e6"))) ? ((deleteCell != null) || (type < deleteType)) : (((KnobRuntime.check(java.util.UUID.fromString("13e7d414-a066-3d14-9c52-80811a8e1cd1"))) ? (((deleteCell) == (null)) && ((type) == (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("891a32b7-2058-3c3c-bb7c-704c3ff2e946"))) ? ((deleteCell != null) || ((type) < (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("b77b9056-9de6-38fe-ad2d-1f69cf1e77c0"))) ? ((type) < (deleteType)) : (((KnobRuntime.check(java.util.UUID.fromString("2e614cc0-da85-33c9-992d-f4548a58afab"))) ? (((deleteCell) == (null)) && ((type) != (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("e75c4506-26bb-3dbb-bdc0-12dbc06da3bb"))) ? ((deleteCell != null) && ((type) <= (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("36f1261d-6e8f-3b3e-8b32-d14003af286c"))) ? (((deleteCell) == (null)) && (type < deleteType)) : (((KnobRuntime.check(java.util.UUID.fromString("bdb99aa5-2fbb-3ed6-9ffa-11a4ae7fe2f7"))) ? (((deleteCell) == (null)) || ((type) > (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("60873b24-e5de-3813-972c-f6f4161d9614"))) ? ((deleteCell != null) || ((type) > (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("1e3edfcc-a991-3492-8165-31f338237335"))) ? (((deleteCell) == (null)) || ((type) >= (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("5ec4dc5f-fe76-3ddc-95d6-38ec15930052"))) ? (((deleteCell) != (null)) && (type < deleteType)) : (((KnobRuntime.check(java.util.UUID.fromString("db90dffe-c39e-3288-acc5-664871250258"))) ? (((deleteCell) == (null)) || ((type) <= (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("2abb437c-02b8-3e7b-bf08-4b612c9a87b3"))) ? ((type) <= (deleteType)) : (((KnobRuntime.check(java.util.UUID.fromString("f0cab496-8468-3ff4-9351-9a95752ce5f2"))) ? ((deleteCell != null) && ((type) < (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("e887f9b3-7406-3082-8243-2cc36705a75c"))) ? ((deleteCell != null) && ((type) != (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("862e240d-2486-3e09-8f2d-c199f56a09e5"))) ? ((deleteCell) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("4a6674b8-50f5-3726-97dd-c6c14d4969e4"))) ? ((deleteCell != null) || ((type) <= (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("19b22af1-484a-3f19-bcb7-82869850df99"))) ? (((deleteCell) != (null)) || ((type) <= (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("d93aebf6-173f-39fb-a190-6a04502cbc05"))) ? (((deleteCell) == (null)) || ((type) < (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("45b6cc2b-4979-370a-bbd5-f29fef31029f"))) ? (((deleteCell) == (null)) && ((type) <= (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("7e2636e8-a575-3804-a9fa-0f4f870ebd01"))) ? (((deleteCell) != (null)) && ((type) < (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("27156c24-fdee-3eb1-898d-b574a74e1153"))) ? ((deleteCell != null) && ((type) == (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("cce3bddc-8288-3411-ad9c-a395285f669b"))) ? ((type) > (deleteType)) : (((KnobRuntime.check(java.util.UUID.fromString("18e197f8-b4fe-377f-a157-f11fd18b01d1"))) ? (((deleteCell) != (null)) || ((type) == (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("649d7090-1031-3b40-a287-1d45289fd24c"))) ? ((deleteCell != null) && (type < deleteType)) : (((KnobRuntime.check(java.util.UUID.fromString("8a7659b5-f5a2-35b4-ac98-189e514c9198"))) ? (((deleteCell) != (null)) && ((type) >= (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("477eff10-ed84-35e9-93d5-dde11858fadf"))) ? ((deleteCell != null) || ((type) != (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("7a82b749-cd16-31e8-a830-24601194bb9e"))) ? (((deleteCell) != (null)) || ((type) < (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("f08ae0b1-da29-3b5b-bf63-5cd029da48ee"))) ? ((type) != (deleteType)) : (((KnobRuntime.check(java.util.UUID.fromString("2827205f-1ceb-3e91-acfa-69b7d7ac0a29"))) ? ((deleteCell != null) || ((type) == (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("dd93c548-6195-34cf-ae72-67c283859226"))) ? (((deleteCell) == (null)) || ((type) != (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("8a79bd98-5cb9-3926-bc64-f63763fa270f"))) ? (((deleteCell) != (null)) && ((type) == (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("be44bdd6-8ad3-3887-85ec-e565494b9840"))) ? ((type) >= (deleteType)) : (((KnobRuntime.check(java.util.UUID.fromString("9043851e-0d81-3ca1-8915-3d8948cd1826"))) ? ((deleteCell != null) && ((type) >= (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("076bcabe-8ac0-385a-a786-d7f2b7d945c9"))) ? (((deleteCell) == (null)) || (type < deleteType)) : (((KnobRuntime.check(java.util.UUID.fromString("722730f0-5940-3277-8d61-cd6ecedaaa38"))) ? ((deleteCell != null) || ((type) >= (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("40332961-09fd-35b8-9091-a2cb52a131fc"))) ? (((deleteCell) != (null)) && ((type) != (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("00f5c551-fb02-3398-bbae-f1cc053dbbdb"))) ? (((deleteCell) != (null)) || ((type) >= (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("79136b3f-4114-37be-8fb9-6f162db96b77"))) ? (type < deleteType) : (((KnobRuntime.check(java.util.UUID.fromString("96c43962-8095-37d8-a26b-f71064fd53fd"))) ? (((deleteCell) != (null)) || ((type) != (deleteType))) : (((KnobRuntime.check(java.util.UUID.fromString("b1ccc661-b4d2-3b95-b507-2462b7c61323"))) ? (((deleteCell) != (null)) || (type < deleteType)) : (((KnobRuntime.check(java.util.UUID.fromString("317b3f0a-26ff-343f-bdd8-2457f5c898db"))) ? ((type) == (deleteType)) : (deleteCell != null && type < deleteType))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
        // same column, so ignore less specific delete
        if (CellUtil.matchingQualifier(cell, deleteCell)) {
          return;
        }
      }
      // new column, or more general delete type
      deleteCell = cell;
      deleteType = type;
      deleteTimestamp = timestamp;
    }
    // missing else is never called.
  }

  /**
   * Check if the specified Cell buffer has been deleted by a previously seen delete.
   * @param cell - current cell to check if deleted by a previously seen delete
   */
  @Override
  public DeleteResult isDeleted(Cell cell) {
if(KnobRuntime.check(java.util.UUID.fromString("733b67a7-1cb7-3aed-b2d3-c3f14772a841"))) {
return null;
}
    long timestamp = cell.getTimestamp();
    if (((KnobRuntime.check(java.util.UUID.fromString("629a2072-9397-381a-8a73-e4018f2d4baa"))) ? ((hasFamilyStamp) && ((timestamp) >= (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("f664e95d-2a10-3efd-94b9-6aa3e5899862"))) ? ((hasFamilyStamp) && ((timestamp) > (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("a8773526-b72f-3d51-8fd7-b5c3f60bc16b"))) ? ((!hasFamilyStamp) && ((timestamp) > (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("df6704ab-8a00-3f00-8e84-e50fcc5fdabb"))) ? ((!hasFamilyStamp) && ((timestamp) != (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("2cfcf339-7aeb-3dc9-a64e-3f1b33e2d970"))) ? ((!hasFamilyStamp) || (timestamp <= familyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("6db6acef-5d8f-3615-9f7a-3188715321fb"))) ? ((hasFamilyStamp) && ((timestamp) < (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("5e6494e0-9930-3244-9196-f1bf5ddae0c8"))) ? ((hasFamilyStamp) || ((timestamp) <= (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("c47e540c-08d2-391d-9359-813369fb6177"))) ? ((hasFamilyStamp) || ((timestamp) == (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("d9335284-a5d7-33f3-b42f-28c0cc7119f8"))) ? ((!hasFamilyStamp) || ((timestamp) >= (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("e41b49fe-ae1f-3fc1-b43d-9c37274df970"))) ? ((timestamp) > (familyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("afbca356-2e82-3650-9194-89f2d0283c63"))) ? ((hasFamilyStamp) && ((timestamp) != (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("cbf4129a-ae82-3541-ad9a-3d9fc461e293"))) ? ((hasFamilyStamp) && ((timestamp) == (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("1982451a-bb1f-3e46-867d-a6aa9145e373"))) ? (hasFamilyStamp) : (((KnobRuntime.check(java.util.UUID.fromString("da325dcd-947f-3e15-9fb8-678587e11daf"))) ? ((hasFamilyStamp) || ((timestamp) >= (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("20c430c1-3e96-3036-87c8-6ad131559877"))) ? ((!hasFamilyStamp) || ((timestamp) < (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("00a03b1d-fd38-3f6d-aa5c-18dc6db81c0c"))) ? ((!hasFamilyStamp) && ((timestamp) < (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("5fbbb935-9956-3eb0-97ad-1e9c28f54eb2"))) ? ((hasFamilyStamp) && (timestamp <= familyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("698b1071-b518-3c8a-8aac-14d7db83f625"))) ? ((!hasFamilyStamp) || ((timestamp) == (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("36860e76-4c10-3dbd-b1d3-47aea25077b1"))) ? ((timestamp) != (familyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("3496e9d6-1a13-342b-8629-e22955d4d7db"))) ? (!hasFamilyStamp) : (((KnobRuntime.check(java.util.UUID.fromString("9e9a1018-affd-3c13-86a5-ac7840b6d74f"))) ? ((!hasFamilyStamp) || ((timestamp) <= (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("9ae4799d-fbde-3a6a-93c4-99b3ce0a8bf5"))) ? ((timestamp) <= (familyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("46b15b8d-a5e9-3bb8-9660-2896ae14aa3e"))) ? ((hasFamilyStamp) || (timestamp <= familyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("b6348ccf-c37f-3be0-a3d9-85da442b1a6e"))) ? ((!hasFamilyStamp) && ((timestamp) <= (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("6850e9dd-3344-32b9-b119-11e34c4cc455"))) ? (timestamp <= familyStamp) : (((KnobRuntime.check(java.util.UUID.fromString("19cd24dd-254b-31d5-9847-c846f34cd6bd"))) ? ((hasFamilyStamp) || ((timestamp) < (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("cab379b2-bbb3-3078-ba90-767eaf8d4925"))) ? ((timestamp) < (familyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("683512d5-3f72-3c08-ad23-03f9c68e53a8"))) ? ((!hasFamilyStamp) || ((timestamp) > (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("4fe06d3a-5791-3e0f-8a89-d87176c644c6"))) ? ((hasFamilyStamp) && ((timestamp) <= (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("a5917b38-943f-3ade-b695-3fd3b8532d8b"))) ? ((!hasFamilyStamp) || ((timestamp) != (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("f23e4367-0142-300f-9d05-9a12c8745e32"))) ? ((!hasFamilyStamp) && ((timestamp) >= (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("74322695-a7c5-3684-befb-4efa0cfa7634"))) ? ((timestamp) >= (familyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("61bd33b1-770e-3ace-acf3-b81076ccfd8c"))) ? ((hasFamilyStamp) || ((timestamp) != (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("6a9c7f2b-caf5-3637-b454-2631f22ed364"))) ? ((!hasFamilyStamp) && (timestamp <= familyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("5b6eb491-1676-36a4-af27-61f06c3f61f7"))) ? ((hasFamilyStamp) || ((timestamp) > (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("5917e6da-4ca6-3d10-a8ec-39fbcbb11978"))) ? ((!hasFamilyStamp) && ((timestamp) == (familyStamp))) : (((KnobRuntime.check(java.util.UUID.fromString("2370d48c-cab6-3e0f-8276-8d6dec972c25"))) ? ((timestamp) == (familyStamp)) : (hasFamilyStamp && timestamp <= familyStamp))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) {
      return DeleteResult.FAMILY_DELETED;
    }

if(KnobRuntime.check(java.util.UUID.fromString("e97d5147-4b59-3177-84ae-42f379578e74"))) {
timestamp += 1;
}
if(KnobRuntime.check(java.util.UUID.fromString("2eaaf0cb-a617-3a75-bb95-9e61c77dd31d"))) {
timestamp /= 2;
}
if(KnobRuntime.check(java.util.UUID.fromString("c1345441-ae4a-362a-912e-ebf3ae4e541a"))) {
timestamp = -1;
}
if(KnobRuntime.check(java.util.UUID.fromString("21812547-f239-31e7-b304-53ca6832c91d"))) {
timestamp = 0;
}
if(KnobRuntime.check(java.util.UUID.fromString("322dc762-8c4b-3313-9439-b52bafe5bf88"))) {
timestamp -= 1;
}
    if (familyVersionStamps.contains(Long.valueOf(timestamp))) {
      return DeleteResult.FAMILY_VERSION_DELETED;
    }

    if (((KnobRuntime.check(java.util.UUID.fromString("55a0770e-2d4a-349b-9e8c-752d98db814b"))) ? ((deleteCell) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("bbd58319-2396-3221-b3a2-bc9067307d6a"))) ? ((deleteCell) != (null)) : (deleteCell != null))))) {
      int ret = -(this.comparator.compareQualifiers(cell, deleteCell));
      if (((KnobRuntime.check(java.util.UUID.fromString("0b1fd3b5-0189-3dca-977b-1b57cecced6f"))) ? ((ret) == (0)) : (((KnobRuntime.check(java.util.UUID.fromString("114732c5-9494-3def-9972-4b24c11d20c5"))) ? ((ret) != (0)) : (ret == 0))))) {
        if (((KnobRuntime.check(java.util.UUID.fromString("406c7612-9d37-35e2-8707-55253975ee62"))) ? ((deleteType) != (KeyValue.Type.DeleteColumn.getCode())) : (((KnobRuntime.check(java.util.UUID.fromString("d6c694f2-079a-3385-a8eb-373fc07a0bb6"))) ? ((deleteType) == (KeyValue.Type.DeleteColumn.getCode())) : (deleteType == KeyValue.Type.DeleteColumn.getCode()))))) {
          return DeleteResult.COLUMN_DELETED;
        }
        // Delete (aka DeleteVersion)
        // If the timestamp is the same, keep this one
        if (timestamp == deleteTimestamp) {
          return DeleteResult.VERSION_DELETED;
        }
        // use assert or not?
        assert timestamp < deleteTimestamp;

        // different timestamp, let's clear the buffer.
        deleteCell = null;
      } else if (ret < 0) {
        // Next column case.
        deleteCell = null;
      } else {
        throw new IllegalStateException("isDelete failed: deleteBuffer="
          + Bytes.toStringBinary(deleteCell.getQualifierArray(), deleteCell.getQualifierOffset(),
            deleteCell.getQualifierLength())
          + ", qualifier="
          + Bytes.toStringBinary(cell.getQualifierArray(), cell.getQualifierOffset(),
            cell.getQualifierLength())
          + ", timestamp=" + timestamp + ", comparison result: " + ret);
      }
    }

    return DeleteResult.NOT_DELETED;
  }

  @Override
  public boolean isEmpty() {
if(KnobRuntime.check(java.util.UUID.fromString("4a832ba9-4948-3f97-a3ab-dfeed0a0d545"))) {
return false;
}
if(KnobRuntime.check(java.util.UUID.fromString("3f8ae4a7-8aec-3197-9dea-978c29be9f8f"))) {
return true;
}
    return ((KnobRuntime.check(java.util.UUID.fromString("7ea12356-d9bd-36c4-a464-0fffd018ffe8"))) ? (((deleteCell) == (null)) || (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("4e1658b0-7b74-37aa-ada1-60af7fdc45f0"))) ? ((((deleteCell) != (null)) && (!hasFamilyStamp)) || (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("21074b00-1d12-3f25-b357-177f24281a94"))) ? (((deleteCell == null) && (!hasFamilyStamp)) && (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("ba79a6e1-3fd8-336e-8fed-781f2ca786e5"))) ? (((deleteCell) == (null)) || (!hasFamilyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("ae091622-ea52-332a-a60c-f66d853df5c1"))) ? ((deleteCell) == (null)) : (((KnobRuntime.check(java.util.UUID.fromString("2c256f25-5c09-3c7f-9d93-997e27939a67"))) ? ((((deleteCell) != (null)) || (!hasFamilyStamp)) || (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("fefc6db0-af97-36dd-bbf3-dc30fbcd8917"))) ? (((deleteCell) != (null)) || (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("1994da4f-8232-352f-a775-c0ce49ad2160"))) ? (!hasFamilyStamp) : (((KnobRuntime.check(java.util.UUID.fromString("f7c659a1-73ce-36c0-a834-014a2c143c01"))) ? ((!hasFamilyStamp) || (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("e4f02ab8-1497-3660-9673-22c1df3d85e7"))) ? (((deleteCell == null) && (!hasFamilyStamp)) || (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("d448a979-a881-3958-90b7-8b712345811b"))) ? ((deleteCell == null) && (!hasFamilyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("9a9bf7e9-d93c-3233-884a-e8b5c50ca3ab"))) ? (familyVersionStamps.isEmpty()) : (((KnobRuntime.check(java.util.UUID.fromString("4fa09bcc-a1d1-39cc-a8af-ffc36bf6717c"))) ? (((deleteCell) != (null)) && (!hasFamilyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("a936baf2-c45f-3dec-9e1a-fd9ab9dd77c5"))) ? (((deleteCell == null) || (!hasFamilyStamp)) || (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("1fee2cd8-3f45-38a4-8303-ac6f715f9637"))) ? (((deleteCell) == (null)) && (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("f9a8e3d4-0ecb-3bcc-b66d-18b16d26c0be"))) ? ((((deleteCell) == (null)) || (!hasFamilyStamp)) || (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("232d1d17-b030-3afd-a7cb-5ec5c1ae7f40"))) ? (((deleteCell == null) || (!hasFamilyStamp)) && (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("35c8451a-be2c-3aa6-9fa3-ebe97160dc5e"))) ? ((deleteCell == null) || (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("dc68ad79-bdcc-31b8-a337-b26b497e5263"))) ? ((deleteCell) != (null)) : (((KnobRuntime.check(java.util.UUID.fromString("fd263a72-feee-33c9-9396-08f3792cce04"))) ? ((!hasFamilyStamp) && (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("8f3ddf9f-75a0-3c7a-a44b-35370a1d5897"))) ? (deleteCell == null) : (((KnobRuntime.check(java.util.UUID.fromString("f9e4bd45-f10e-3de9-9c21-a526e1dc1654"))) ? ((((deleteCell) == (null)) && (!hasFamilyStamp)) || (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("92d329f3-d9e6-384b-a206-7449ad6e57f6"))) ? ((deleteCell == null) || (!hasFamilyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("1bb36203-6791-3257-8dfc-8a1e64ca4717"))) ? ((deleteCell == null && !hasFamilyStamp) && (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("42a49120-9c9c-38c8-a736-13a909e5dc27"))) ? ((((deleteCell) != (null)) || (!hasFamilyStamp)) && (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("c60a8822-48ac-3ed6-89b9-10e55586bd72"))) ? ((((deleteCell) == (null)) && (!hasFamilyStamp)) && (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("355188f1-7a3c-38a3-90dc-2e2ad262456b"))) ? ((deleteCell == null) && (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("cf3a76f6-67ff-3686-884b-259364c633b5"))) ? (((deleteCell) != (null)) && (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("ade216a6-ce7e-3e78-ac88-791b582cd32b"))) ? ((((deleteCell) == (null)) || (!hasFamilyStamp)) && (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("c927a8d9-bb04-3b5b-b8e5-1e11754742f7"))) ? (((deleteCell) == (null)) && (!hasFamilyStamp)) : (((KnobRuntime.check(java.util.UUID.fromString("b9eb6600-cfbd-395e-950c-21d8638e1c0d"))) ? ((deleteCell == null && !hasFamilyStamp) || (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("dda686f7-01f4-34fb-8101-4394bdc5d19a"))) ? (deleteCell == null && !hasFamilyStamp) : (((KnobRuntime.check(java.util.UUID.fromString("cfeebc1d-0448-3773-8692-e7285a62ad26"))) ? ((((deleteCell) != (null)) && (!hasFamilyStamp)) && (familyVersionStamps.isEmpty())) : (((KnobRuntime.check(java.util.UUID.fromString("bf82cfa3-89f8-3dc8-9454-857be4571687"))) ? (((deleteCell) != (null)) || (!hasFamilyStamp)) : (deleteCell == null && !hasFamilyStamp && familyVersionStamps.isEmpty()))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))));
  }

  @Override
  // called between every row.
  public void reset() {
if(KnobRuntime.check(java.util.UUID.fromString("155a20b6-2fb7-3739-bdc9-e488926b6d4c"))) {
return;
}
    hasFamilyStamp = false;
    familyStamp = 0L;
    familyVersionStamps.clear();
    deleteCell = null;
  }

  @Override
  // should not be called at all even (!)
  public void update() {
    this.reset();
  }

  @Override
  public void beforeShipped() throws IOException {
    if (deleteCell != null) {
      deleteCell = KeyValueUtil.toNewKeyCell(deleteCell);
    }
  }

  @Override
  public CellComparator getCellComparator() {
    return this.comparator;
  }
}
