package org.hammerlab.guacamole.variants

import org.hammerlab.genomics.reads.ReadsUtil
import org.hammerlab.guacamole.pileup.{ Util ⇒ PileupUtil }
import org.hammerlab.guacamole.reference.ReferenceUtil
import org.hammerlab.guacamole.util.GuacFunSuite

class AlleleEvidenceSuite
  extends GuacFunSuite
    with ReadsUtil
    with PileupUtil
    with ReferenceUtil {

  override lazy val reference = makeReference(sc, "chr1", 0, "NTAGATCGA")

  test("allele evidence from pileup, all reads support") {
    val reads =
      Seq(
        makeRead("TCGATCGA", "8M", 1, alignmentQuality = 30),
        makeRead("TCGATCGA", "8M", 1, alignmentQuality = 30),
        makeRead("TCGACCCTCGA", "4M3I4M", 1, alignmentQuality = 60)
      )

    val variantPileup = makePileup(reads, "chr1", 2)

    val variantEvidence = AlleleEvidence(
      likelihood = 0.5,
      allele = Allele("A", "C"),
      variantPileup
    )

    variantEvidence.meanMappingQuality should === (40.0)
    variantEvidence.medianMappingQuality should === (30)
    variantEvidence.medianMismatchesPerRead should === (1)
  }

  test("allele evidence from pileup, one read supports") {
    val reads =
      Seq(
        makeRead("TAGATCGA", "8M", 1, alignmentQuality = 30),
        makeRead("TCGATCGA", "8M", 1, alignmentQuality = 60),
        makeRead("TAGACCCTCGA", "4M3I4M", 1, alignmentQuality = 60)
      )

    val variantPileup = makePileup(reads, "chr1", 2)

    val variantEvidence = AlleleEvidence(
      likelihood = 0.5,
      allele = Allele("A", "C"),
      variantPileup
    )

    variantEvidence.meanMappingQuality should === (60.0)
    variantEvidence.medianMappingQuality should === (60)
    variantEvidence.medianMismatchesPerRead should === (1)
  }

  test("allele evidence from pileup, no read supports") {
    val reads =
      Seq(
        makeRead("TAGATCGA", "8M", 1, alignmentQuality = 30),
        makeRead("TAGATCGA", "8M", 1, alignmentQuality = 60),
        makeRead("TAGACCCTCGA", "4M3I4M", 1, alignmentQuality = 60)
      )

    val variantPileup = makePileup(reads, "chr1", 2)

    val variantEvidence = AlleleEvidence(
      likelihood = 0.5,
      allele = Allele("A", "C"),
      variantPileup
    )

    variantEvidence.meanMappingQuality.toString should === ("NaN")
    variantEvidence.medianMappingQuality.toString should === ("NaN")
    variantEvidence.medianMismatchesPerRead.toString should === ("NaN")
  }
}
