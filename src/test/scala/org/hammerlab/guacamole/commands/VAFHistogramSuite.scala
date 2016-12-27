package org.hammerlab.guacamole.commands

import org.hammerlab.guacamole.util.GuacFunSuite

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

    VAFHistogram.generateVAFHistograms(loci, 10) === (
      Map(
        0 ->
          Vector(
            20 -> 1,
            40 -> 1,
            50 -> 1
          ),
        1 ->
          Vector(
            30 -> 1,
            50 -> 1
          )
      )
    )

    VAFHistogram.generateVAFHistograms(loci, 20) === (
      Map(
        0 ->
          Vector(
            25 -> 1,
            40 -> 1,
            55 -> 1
          ),
        1 ->
          Vector(
            35 -> 1,
            50 -> 1
          )
      )
    )

    VAFHistogram.generateVAFHistograms(loci, 100) === (
      Map(
        0 ->
          Vector(
            25 -> 1,
            40 -> 1,
            55 -> 1
          ),
        1 ->
          Vector(
            35 -> 1,
            50 -> 1
          )
      )
    )
  }
}
