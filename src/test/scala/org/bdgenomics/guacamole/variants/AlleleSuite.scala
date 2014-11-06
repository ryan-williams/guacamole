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

package org.bdgenomics.guacamole.variants

import org.bdgenomics.guacamole.{ TestUtil, Bases }
import org.scalatest.{ Matchers, FunSuite }

class AlleleSuite extends FunSuite with Matchers {

  test("isVariant") {
    val mismatch = Allele(debox.Buffer(Bases.T), debox.Buffer(Bases.A))
    mismatch.isVariant should be(true)

    val reference = Allele(debox.Buffer(Bases.T), debox.Buffer(Bases.T))
    reference.isVariant should be(false)

    val deletion = Allele(debox.Buffer(Bases.T, Bases.T, Bases.T), debox.Buffer(Bases.T))
    deletion.isVariant should be(true)

    val insertion = Allele(debox.Buffer(Bases.T), debox.Buffer(Bases.T, Bases.A, Bases.A))
    insertion.isVariant should be(true)

  }

  test("serializing allele") {
    val a1 = Allele(debox.Buffer(Bases.T), debox.Buffer(Bases.A))
    val a1Serialized = TestUtil.serialize(a1)
    val a1Deserialized = TestUtil.deserialize[Allele](a1Serialized)

    assert(a1 === a1Deserialized)

    val a2 = Allele(debox.Buffer(Bases.T, Bases.T, Bases.C), debox.Buffer(Bases.A))
    val a2Serialized = TestUtil.serialize(a2)
    val a2Deserialized = TestUtil.deserialize[Allele](a2Serialized)

    assert(a2 === a2Deserialized)
  }

}
