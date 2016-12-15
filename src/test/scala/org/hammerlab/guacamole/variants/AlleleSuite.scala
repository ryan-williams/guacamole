package org.hammerlab.guacamole.variants

import org.hammerlab.genomics.bases.Base.{ A, C, T }
import org.hammerlab.genomics.bases.Bases
import org.hammerlab.guacamole.util.GuacFunSuite
import org.scalatest.Matchers

class AlleleSuite extends GuacFunSuite {

  test("isVariant") {
    val mismatch = Allele(Bases(T), Bases(A))
    mismatch.isVariant === (true)

    val reference = Allele(Bases(T), Bases(T))
    reference.isVariant === (false)

    val deletion = Allele(Bases(T, T, T), Bases(T))
    deletion.isVariant === (true)

    val insertion = Allele(Bases(T), Bases(T, A, A))
    insertion.isVariant === (true)

  }

  test("serializing allele") {
    val a1 = Allele(Bases(T), Bases(A))

    val a1Serialized = serialize(a1)
    val a1Deserialized = deserialize[Allele](a1Serialized)

    assert(a1 === a1Deserialized)

    val a2 = Allele(Bases(T, T, C), Bases(A))
    val a2Serialized = serialize(a2)
    val a2Deserialized = deserialize[Allele](a2Serialized)

    assert(a2 === a2Deserialized)
  }

}
