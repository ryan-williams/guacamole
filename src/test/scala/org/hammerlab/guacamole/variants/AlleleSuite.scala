package org.hammerlab.guacamole.variants

import org.hammerlab.genomics.bases.Base.{ A, C, T }
import org.hammerlab.genomics.bases.Bases
import org.hammerlab.guacamole.util.GuacFunSuite

class AlleleSuite extends GuacFunSuite {

  test("isVariant") {
    val mismatch = Allele(Bases(T), Bases(A))
    mismatch.isVariant should === (true)

    val reference = Allele(Bases(T), Bases(T))
    reference.isVariant should === (false)

    val deletion = Allele(Bases(T, T, T), Bases(T))
    deletion.isVariant should === (true)

    val insertion = Allele(Bases(T), Bases(T, A, A))
    insertion.isVariant should === (true)

  }

  test("serializing allele") {
    val a1 = Allele(Bases(T), Bases(A))

    val a1Serialized = serialize(a1)
    val a1Deserialized = deserialize[Allele](a1Serialized)

    a1 should ===(a1Deserialized)

    val a2 = Allele(Bases(T, T, C), Bases(A))
    val a2Serialized = serialize(a2)
    val a2Deserialized = deserialize[Allele](a2Serialized)

    a2 should ===(a2Deserialized)
  }
}
