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
package org.apache.hadoop.hbase.io.hfile;
import org.knobinjection.runtime.KnobRuntime;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDecodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultDecodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockDefaultEncodingContext;
import org.apache.hadoop.hbase.io.encoding.HFileBlockEncodingContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Do different kinds of data block encoding according to column family options.
 */
@InterfaceAudience.Private
public class HFileDataBlockEncoderImpl implements HFileDataBlockEncoder {
  private final DataBlockEncoding encoding;

  /**
   * Do data block encoding with specified options.
   * @param encoding What kind of data block encoding will be used.
   */
  public HFileDataBlockEncoderImpl(DataBlockEncoding encoding) {
    this.encoding = encoding != null ? encoding : DataBlockEncoding.NONE;
  }

  public static HFileDataBlockEncoder createFromFileInfo(HFileInfo fileInfo) throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("a875c73d-785c-30dd-9515-71cde3f7337d"))) {
throw new java.io.IOException("Injected exception");
}
    DataBlockEncoding encoding = DataBlockEncoding.NONE;
    byte[] dataBlockEncodingType = fileInfo.get(DATA_BLOCK_ENCODING);
    if (((KnobRuntime.check(java.util.UUID.fromString("72315a48-5691-337d-a9e1-8e0174a490cd"))) ? ((dataBlockEncodingType) != (null)) : (dataBlockEncodingType != null))) {
      String dataBlockEncodingStr = Bytes.toString(dataBlockEncodingType);
      try {
        encoding = DataBlockEncoding.valueOf(dataBlockEncodingStr);
      } catch (IllegalArgumentException ex) {
        throw new IOException(
          "Invalid data block encoding type in file info: " + dataBlockEncodingStr, ex);
      }
    }

    if (encoding == DataBlockEncoding.NONE) {
      return NoOpDataBlockEncoder.INSTANCE;
    }
    return new HFileDataBlockEncoderImpl(encoding);
  }

  @Override
  public void saveMetadata(HFile.Writer writer) throws IOException {
    writer.appendFileInfo(DATA_BLOCK_ENCODING, encoding.getNameInBytes());
  }

  @Override
  public DataBlockEncoding getDataBlockEncoding() {
    return encoding;
  }

  public boolean useEncodedScanner(boolean isCompaction) {
    if (isCompaction && encoding == DataBlockEncoding.NONE) {
      return false;
    }
    return encoding != DataBlockEncoding.NONE;
  }

  @Override
  public DataBlockEncoding getEffectiveEncodingInCache(boolean isCompaction) {
    if (!useEncodedScanner(isCompaction)) {
      return DataBlockEncoding.NONE;
    }
    return encoding;
  }

  @Override
  public void encode(Cell cell, HFileBlockEncodingContext encodingCtx, DataOutputStream out)
    throws IOException {
if(KnobRuntime.check(java.util.UUID.fromString("642bee44-3a7e-33a1-8adc-444f9926abee"))) {
throw new java.io.IOException("Injected exception");
}
    this.encoding.getEncoder().encode(cell, encodingCtx, out);
  }

  @Override
  public boolean useEncodedScanner() {
    return encoding != DataBlockEncoding.NONE;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(encoding=" + encoding + ")";
  }

  @Override
  public HFileBlockEncodingContext newDataBlockEncodingContext(Configuration conf,
    byte[] dummyHeader, HFileContext fileContext) {
if(KnobRuntime.check(java.util.UUID.fromString("1e431c24-a5ca-33b9-9e7d-216ab718c12b"))) {
try {
    java.lang.reflect.Field _knob_field_ = fileContext.getClass().getDeclaredField("blockSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(fileContext));
    _knob_field_.set(fileContext, oldValue / 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    DataBlockEncoder encoder = encoding.getEncoder();
    if (((KnobRuntime.check(java.util.UUID.fromString("5e7db5aa-cb24-352a-9a6f-77d5a8e4db72"))) ? ((encoder) == (null)) : (encoder != null))) {
      return encoder.newDataBlockEncodingContext(conf, encoding, dummyHeader, fileContext);
    }
    return new HFileBlockDefaultEncodingContext(conf, null, dummyHeader, fileContext);
  }

  @Override
  public HFileBlockDecodingContext newDataBlockDecodingContext(Configuration conf,
    HFileContext fileContext) {
if(KnobRuntime.check(java.util.UUID.fromString("db13810e-f2e6-34b5-b320-b3fbebd6a807"))) {
try {
    java.lang.reflect.Field _knob_field_ = fileContext.getClass().getDeclaredField("blockSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(fileContext));
    _knob_field_.set(fileContext, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
    DataBlockEncoder encoder = encoding.getEncoder();
    if (encoder != null) {
if(KnobRuntime.check(java.util.UUID.fromString("cac9ff6b-7104-3823-a972-48d5c2da1478"))) {
try {
    java.lang.reflect.Field _knob_field_ = fileContext.getClass().getDeclaredField("blockSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(fileContext));
    _knob_field_.set(fileContext, oldValue * 2);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e5c806d0-aa9b-3b19-92c8-b5d160abafa4"))) {
try {
    java.lang.reflect.Field _knob_field_ = conf.getClass().getDeclaredField("loadDefaults");
    _knob_field_.setAccessible(true);
    boolean oldValue = (boolean)_knob_field_.get(conf);
    _knob_field_.set(conf, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("1936f7c4-cf6c-3962-99d9-157b37f69a28"))) {
try {
    java.lang.reflect.Field _knob_field_ = fileContext.getClass().getDeclaredField("blockSize");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(fileContext));
    _knob_field_.set(fileContext, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("e0e7e852-83c9-3fe0-851a-fb7fe775f52d"))) {
try {
    java.lang.reflect.Field _knob_field_ = fileContext.getClass().getDeclaredField("usesHBaseChecksum");
    _knob_field_.setAccessible(true);
    boolean oldValue = (boolean)_knob_field_.get(fileContext);
    _knob_field_.set(fileContext, !oldValue);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
if(KnobRuntime.check(java.util.UUID.fromString("df806359-ab8b-3b20-a975-045d29be6cbf"))) {
try {
    java.lang.reflect.Field _knob_field_ = fileContext.getClass().getDeclaredField("bytesPerChecksum");
    _knob_field_.setAccessible(true);
    int oldValue = ((int)_knob_field_.get(fileContext));
    _knob_field_.set(fileContext, oldValue + 1);
} catch (java.lang.Exception _e_) {
    // Reflection access failed
    _e_.printStackTrace();
}
}
      return encoder.newDataBlockDecodingContext(conf, fileContext);
    }
    return new HFileBlockDefaultDecodingContext(conf, fileContext);
  }

  @Override
  public void startBlockEncoding(HFileBlockEncodingContext encodingCtx, DataOutputStream out)
    throws IOException {
    if (((KnobRuntime.check(java.util.UUID.fromString("47dc2a3f-6c9a-3d61-9406-90dab5317dff"))) ? (((this.encoding) != (null)) || (this.encoding != DataBlockEncoding.NONE)) : (((KnobRuntime.check(java.util.UUID.fromString("6db79b06-0dff-3bd5-ab13-704bfc0ec4a3"))) ? ((this.encoding) != (DataBlockEncoding.NONE)) : (this.encoding != null && this.encoding != DataBlockEncoding.NONE))))) {
      this.encoding.getEncoder().startBlockEncoding(encodingCtx, out);
    }
  }

  @Override
  public void endBlockEncoding(HFileBlockEncodingContext encodingCtx, DataOutputStream out,
    byte[] uncompressedBytesWithHeader, BlockType blockType) throws IOException {
    this.encoding.getEncoder().endBlockEncoding(encodingCtx, out, uncompressedBytesWithHeader);
  }
}
