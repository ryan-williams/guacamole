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

package org.bdgenomics.guacamole.commands

import org.bdgenomics.guacamole.TestUtil
import org.bdgenomics.guacamole.TestUtil.SparkFunSuite
import org.bdgenomics.guacamole.filters.SomaticGenotypeFilter
import org.bdgenomics.guacamole.pileup.Pileup
import org.bdgenomics.guacamole.reads.MappedRead
import org.scalatest.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class SomaticStandardCallerSuite extends SparkFunSuite with Matchers with TableDrivenPropertyChecks {

  def loadPileup(filename: String, locus: Long = 0): Pileup = {
    val records = TestUtil.loadReads(sc, filename).mappedReads
    val localReads = records.collect
    Pileup(localReads, locus)
  }

  /**
   * Common algorithm parameters - fixed for all tests
   */
  val logOddsThreshold = 120
  val minAlignmentQuality = 1
  val minTumorReadDepth = 8
  val minNormalReadDepth = 4
  val maxTumorReadDepth = 200
  val minTumorAlternateReadDepth = 3
  val maxMappingComplexity = 20
  val minAlignmentForComplexity = 1

  val filterMultiAllelic = false

  val minLikelihood = 70
  val minVAF = 5

  def testVariants(tumorReads: Seq[MappedRead], normalReads: Seq[MappedRead], positions: Array[Long], shouldFindVariant: Boolean = false) = {
    val positionsTable = Table("locus", positions: _*)
    forAll(positionsTable) {
      (locus: Long) =>
        val (tumorPileup, normalPileup) = TestUtil.loadTumorNormalPileup(tumorReads, normalReads, locus)

        val calledGenotypes = SomaticStandardCaller.findPotentialVariantAtLocus(
          tumorPileup,
          normalPileup,
          logOddsThreshold,
          maxMappingComplexity,
          minAlignmentForComplexity,
          minAlignmentQuality,
          filterMultiAllelic
        )

        val foundVariant = SomaticGenotypeFilter(
          calledGenotypes,
          minTumorReadDepth,
          maxTumorReadDepth,
          minNormalReadDepth,
          minTumorAlternateReadDepth,
          logOddsThreshold,
          minVAF = minVAF,
          minLikelihood).size > 0

        foundVariant should be(shouldFindVariant)
    }
  }

  sparkTest("testing simple positive variants") {
    val (tumorReads, normalReads) = TestUtil.loadTumorNormalReads(sc, "tumor.chr20.tough.sam", "normal.chr20.tough.sam")
    val positivePositions = Array[Long](42999694, 25031215, 44061033, 45175149, 755754, 1843813,
      3555766, 3868620, 9896926, 14017900, 17054263, 35951019, 50472935, 51858471, 58201903, 7087895,
      19772181, 30430960, 32150541, 42186626, 44973412, 46814443, 52311925, 53774355, 57280858, 62262870)
    testVariants(tumorReads, normalReads, positivePositions, shouldFindVariant = true)
  }

  sparkTest("testing simple negative variants on syn1") {
    val (tumorReads, normalReads) = TestUtil.loadTumorNormalReads(sc,
      "synthetic.challenge.set1.tumor.v2.withMDTags.chr2.syn1fp.sam",
      "synthetic.challenge.set1.normal.v2.withMDTags.chr2.syn1fp.sam")
    val negativePositions = Array[Long](216094721, 3529313, 8789794, 104043280, 104175801,
      126651101, 241901237, 57270796, 120757852)
    testVariants(tumorReads, normalReads, negativePositions, shouldFindVariant = false)
  }

  sparkTest("testing complex region negative variants on syn1") {
    val (tumorReads, normalReads) = TestUtil.loadTumorNormalReads(sc,
      "synthetic.challenge.set1.tumor.v2.withMDTags.chr2.complexvar.sam",
      "synthetic.challenge.set1.normal.v2.withMDTags.chr2.complexvar.sam")
    val negativePositions = Array[Long](148487667, 134307261, 90376213, 3638733, 112529049, 91662497, 109347468)
    testVariants(tumorReads, normalReads, negativePositions, shouldFindVariant = false)

    val positivePositions = Array[Long](82949713, 130919744)
    testVariants(tumorReads, normalReads, positivePositions, shouldFindVariant = true)
  }

  sparkTest("difficult negative variants") {

    val (tumorReads, normalReads) = TestUtil.loadTumorNormalReads(sc, "tumor.chr20.simplefp.sam", "normal.chr20.simplefp.sam")
    val negativeVariantPositions = Array[Long](26211835, 29652479, 54495768, 13046318, 25939088)
    testVariants(tumorReads, normalReads, negativeVariantPositions, shouldFindVariant = false)
  }
}
