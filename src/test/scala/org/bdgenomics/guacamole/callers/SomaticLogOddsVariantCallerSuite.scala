package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole.{ MappedRead, TestUtil }
import org.bdgenomics.guacamole.TestUtil.SparkFunSuite
import org.bdgenomics.guacamole.pileup.Pileup
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.prop.{ TableDrivenPropertyChecks, GeneratorDrivenPropertyChecks }
import org.scalacheck.Gen

class SomaticLogOddsVariantCallerSuite extends SparkFunSuite with ShouldMatchers with TableDrivenPropertyChecks {

  def loadPileup(filename: String, locus: Long = 0): Pileup = {
    val records = TestUtil.loadReads(sc, filename).mappedReads
    val localReads = records.collect
    Pileup(localReads, locus)
  }

  /**
   * Common algorithm parameters - fixed for all tests
   */
  val logOddsThreshold = 97
  val minLikelihood = 30
  val minAlignmentQuality = 1
  val minReadDepth = 6
  val minAlternateReadDepth = 1
  val maxNormalAlternateReadDepth = 5
  val maxMappingComplexity = 100
  val minAlignmentForComplexity = 30
  val maxPercentAbnormalInsertSize = 100
  val lowStrandBiasLimit = 130

  def testVariants(tumorReads: Seq[MappedRead], normalReads: Seq[MappedRead], positions: Array[Long], isTrue: Boolean = false) = {
    val positionsTable = Table("locus", positions: _*)
    forAll(positionsTable) {
      (locus: Long) =>
        val (tumorPileup, normalPileup) = TestUtil.loadTumorNormalPileup(tumorReads, normalReads, locus)
        val calledGenotypes = SomaticLogOddsVariantCaller.callSomaticVariantsAtLocus(tumorPileup,
          normalPileup,
          minLikelihood,
          logOddsThreshold,
          minAlignmentQuality,
          lowStrandBiasLimit,
          100,
          maxNormalAlternateReadDepth,
          maxMappingComplexity,
          minAlignmentForComplexity,
          false,
          false,
          false,
          minReadDepth,
          minAlternateReadDepth,
          maxPercentAbnormalInsertSize)
        val hasVariant = calledGenotypes.size > 0
        hasVariant should be(isTrue)
    }
  }

  sparkTest("testing simple positive variants") {
    val (tumorReads, normalReads) = TestUtil.loadTumorNormalReads(sc, "tumor.chr20.tough.sam", "normal.chr20.tough.sam")
    val positivePositions = Array[Long](49074448, 42999694, 33652547, 25031215, 44061033, 45175149, 755754, 1843813,
      3555766, 3868620, 9896926, 14017900, 17054263, 35951019, 39083205, 50472935, 51858471, 58201903, 7087895,
      19772181, 30430960, 32150541, 42186626, 44973412, 46814443, 50036538, 52311925, 53774355, 57280858, 62262870)
    testVariants(tumorReads, normalReads, positivePositions, isTrue = true)
  }
  //
  //  sparkTest("testing strand bias positive and negative variants") {
  //    val (tumorReads, normalReads) = TestUtil.loadTumorNormalReads(sc, "tumor.chr20.simplefp.sam", "normal.chr20.simplefp.sam")
  //    val negativeStrandBiasPositions = Array[Long](13720384, 60992707, 42248574, 25922747)
  //    testVariants(tumorReads, normalReads, negativeStrandBiasPositions, isTrue = false)
  //
  //    //    val positiveStrandBiasPositions = Array[Long](13720384, 60992707, 42248574, 25922747)
  //    //    testVariants(tumorReads, normalReads, positiveStrandBiasPositions, isTrue = true)
  //  }

  sparkTest("difficult negative variants") {

    //11211310
    //57002286
    val (tumorReads, normalReads) = TestUtil.loadTumorNormalReads(sc, "tumor.chr20.simplefp.sam", "normal.chr20.simplefp.sam")
    val negativeVariantPositions = Array[Long](26211835, 29603746, 29652479, 54495768, 13046318, 25939088)
    testVariants(tumorReads, normalReads, negativeVariantPositions, isTrue = false)
  }
}
