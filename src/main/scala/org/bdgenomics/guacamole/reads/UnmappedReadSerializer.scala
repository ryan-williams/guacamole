/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
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

package org.bdgenomics.guacamole.reads

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }

// Serialization: UnmappedRead
class UnmappedReadSerializer extends Serializer[UnmappedRead] with CanSerializeMatePropertiesOption {
  def write(kryo: Kryo, output: Output, obj: UnmappedRead) = {
    output.writeInt(obj.token)
    assert(obj.sequence.length == obj.baseQualities.length)
    output.writeInt(obj.sequence.length, true)
    output.writeBytes(obj.sequence.elems)
    output.writeBytes(obj.baseQualities.elems)
    output.writeBoolean(obj.isDuplicate)
    output.writeString(obj.sampleName)
    output.writeBoolean(obj.failedVendorQualityChecks)
    output.writeBoolean(obj.isPositiveStrand)

    write(kryo, output, obj.matePropertiesOpt)
  }

  def read(kryo: Kryo, input: Input, klass: Class[UnmappedRead]): UnmappedRead = {
    val token = input.readInt()
    val count: Int = input.readInt(true)
    val sequenceArray: debox.Buffer[Byte] = debox.Buffer.unsafe(input.readBytes(count))
    val qualityScoresArray = debox.Buffer.unsafe(input.readBytes(count))
    val isDuplicate = input.readBoolean()
    val sampleName = input.readString().intern()
    val failedVendorQualityChecks = input.readBoolean()
    val isPositiveStrand = input.readBoolean()

    val matePropertiesOpt = read(kryo, input)

    UnmappedRead(
      token,
      sequenceArray,
      qualityScoresArray,
      isDuplicate,
      sampleName.intern,
      failedVendorQualityChecks,
      isPositiveStrand,
      matePropertiesOpt
    )
  }
}

