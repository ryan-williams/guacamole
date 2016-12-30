package org.hammerlab.guacamole.commands

import org.hammerlab.guacamole.util.GuacFunSuite

import VAFHistogram.generateVAFHistograms

class VAFHistogramSuite extends GuacFunSuite {

  kryoRegister(
    classOf[Array[VariantLocus]],
    classOf[VariantLocus]
  )

  test("generating the histogram") {

    val loci =
      sc.parallelize(
        Seq(
          VariantLocus(0, "chr1", 1, 0.25f),
          VariantLocus(1, "chr1", 2, 0.35f),
          VariantLocus(0, "chr1", 3, 0.4f),
          VariantLocus(1, "chr1", 4, 0.5f),
          VariantLocus(0, "chr1", 5, 0.55f)
        )
      )

    generateVAFHistograms(loci, 10) should === (
      Map(
        0 ->
          Vector(
            20 -> 1L,
            40 -> 1L,
            50 -> 1L
          ),
        1 ->
          Vector(
            30 -> 1L,
            50 -> 1L
          )
      )
    )

    generateVAFHistograms(loci, 20) should === (
      Map(
        0 ->
          Vector(
            25 -> 1L,
            40 -> 1L,
            55 -> 1L
          ),
        1 ->
          Vector(
            35 -> 1L,
            50 -> 1L
          )
      )
    )

    generateVAFHistograms(loci, 100) should === (
      Map(
        0 ->
          Vector(
            25 -> 1L,
            40 -> 1L,
            55 -> 1L
          ),
        1 ->
          Vector(
            35 -> 1L,
            50 -> 1L
          )
      )
    )
  }
}
