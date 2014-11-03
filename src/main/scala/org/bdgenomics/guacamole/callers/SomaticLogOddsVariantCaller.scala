package org.bdgenomics.guacamole.callers

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.cli.Args4j
import org.bdgenomics.guacamole.Common.Arguments.{ Output, TumorNormalReads }
import org.bdgenomics.guacamole._
import org.bdgenomics.guacamole.filters.PileupFilter.PileupFilterArguments
import org.bdgenomics.guacamole.filters.SomaticGenotypeFilter.SomaticGenotypeFilterArguments
import org.bdgenomics.guacamole.filters.{ SomaticGenotypeFilter, PileupFilter, SomaticAlternateReadDepthFilter, SomaticReadDepthFilter }
import org.bdgenomics.guacamole.pileup.Pileup
import org.bdgenomics.guacamole.reads.Read
import org.bdgenomics.guacamole.variants._
import org.kohsuke.args4j.{ Option => Opt }

/**
 * Simple subtraction based somatic variant caller
 *
 * This takes two variant callers, calls variants on tumor and normal independently
 * and outputs the variants in the tumor sample BUT NOT the normal sample
 *
 * This assumes that both read sets only contain a single sample, otherwise we should compare
 * on a sample identifier when joining the genotypes
 *
 */
object SomaticLogOddsVariantCaller extends Command with Serializable with Logging {
  override val name = "logodds-somatic"
  override val description = "call somatic variants using a two independent caller on tumor and normal"

  private class Arguments
      extends DistributedUtil.Arguments
      with Output
      with SomaticGenotypeFilterArguments
      with PileupFilterArguments
      with TumorNormalReads {

    @Opt(name = "-tumorLogOdds", usage = "Minimum log odds threshold for possible variant candidates")
    var tumorLogOdds: Int = 20

    @Opt(name = "-normalLogOdds", usage = "Minimum log odds threshold for possible variant candidates")
    var normalLogOdds: Int = 20

  }

  override def run(rawArgs: Array[String]): Unit = {

    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(appName = Some(name))

    val filters = Read.InputFilters(mapped = true, nonDuplicate = true, passedVendorQualityChecks = true)
    val (tumorReads, normalReads) = Common.loadTumorNormalReadsFromArguments(args, sc, filters)

    assert(tumorReads.sequenceDictionary == normalReads.sequenceDictionary,
      "Tumor and normal samples have different sequence dictionaries. Tumor dictionary: %s.\nNormal dictionary: %s."
        .format(tumorReads.sequenceDictionary, normalReads.sequenceDictionary))

    val maxMappingComplexity = args.maxMappingComplexity
    val minAlignmentForComplexity = args.minAlignmentForComplexity

    val filterMultiAllelic = args.filterMultiAllelic
    val minAlignmentQuality = args.minAlignmentQuality
    val maxReadDepth = args.maxTumorReadDepth

    val loci = Common.loci(args, normalReads)
    val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(
      args,
      loci,
      tumorReads.mappedReads,
      normalReads.mappedReads
    )

    val tumorLogOdds = args.tumorLogOdds
    val normalLogOdds = args.normalLogOdds
    var potentialGenotypes: RDD[CalledSomaticAllele] =
      DistributedUtil.pileupFlatMapTwoRDDs[CalledSomaticAllele](
        tumorReads.mappedReads,
        normalReads.mappedReads,
        lociPartitions,
        skipEmpty = true, // skip empty pileups
        (pileupTumor, pileupNormal) =>
          findPotentialVariantAtLocus(
            pileupTumor,
            pileupNormal,
            tumorLogOdds,
            normalLogOdds,
            maxMappingComplexity,
            minAlignmentForComplexity,
            minAlignmentQuality,
            filterMultiAllelic,
            maxReadDepth
          ).iterator
      )

    // Filter potential genotypes to min read values
    potentialGenotypes =
      SomaticReadDepthFilter(
        potentialGenotypes,
        args.minTumorReadDepth,
        args.maxTumorReadDepth,
        args.minNormalReadDepth
      )

    potentialGenotypes =
      SomaticAlternateReadDepthFilter(
        potentialGenotypes,
        args.minTumorAlternateReadDepth
      )

    potentialGenotypes.persist()
    Common.progress("Computed %,d potential genotypes".format(potentialGenotypes.count))

    val genotypeLociPartitions = DistributedUtil.partitionLociUniformly(args.parallelism, loci)
    //    val genotypes: RDD[CalledSomaticAllele] =
    //      DistributedUtil.windowFlatMapWithState[CalledSomaticAllele, CalledSomaticAllele, Option[String]](
    //        Seq(potentialGenotypes),
    //        genotypeLociPartitions,
    //        skipEmpty = true,
    //        snvWindowRange.toLong,
    //        None,
    //        removeCorrelatedGenotypes
    //      )
    //    genotypes.persist()
    //    Common.progress("Computed %,d genotypes after regional analysis".format(genotypes.count))

    val filteredGenotypes: RDD[CalledSomaticAllele] = SomaticGenotypeFilter(potentialGenotypes, args)
    Common.progress("Computed %,d genotypes after basic filtering".format(filteredGenotypes.count))

    Common.writeVariantsFromArguments(
      args,
      filteredGenotypes.flatMap(AlleleConversions.calledSomaticAlleleToADAMGenotype)
    )

    DelayedMessages.default.print()
  }

  /**
   * Remove genotypes if there are others in a nearby window
   *
   * @param state Unused
   * @param genotypeWindows Collection of potential genotypes in the window
   * @return Set of genotypes if there are no others in the window
   */
  def removeCorrelatedGenotypes(state: Option[String],
                                genotypeWindows: Seq[SlidingWindow[CalledSomaticAllele]]): (Option[String], Iterator[CalledSomaticAllele]) = {
    val genotypeWindow = genotypeWindows(0)
    val locus = genotypeWindow.currentLocus
    val currentGenotypes = genotypeWindow.currentRegions.filter(_.overlapsLocus(locus))

    assert(currentGenotypes.length <= 1, "There cannot be more than one called genotype at the given locus")

    if (currentGenotypes.size == genotypeWindow.currentRegions().size) {
      (None, currentGenotypes.iterator)
    } else {
      (None, Iterator.empty)
    }
  }

  def findPotentialVariantAtLocus(tumorPileup: Pileup,
                                  normalPileup: Pileup,
                                  tumorLogOdds: Int,
                                  normalLogOdds: Int,
                                  maxMappingComplexity: Int = 100,
                                  minAlignmentForComplexity: Int = 1,
                                  minAlignmentQuality: Int = 1,
                                  filterMultiAllelic: Boolean = false,
                                  maxReadDepth: Int = Int.MaxValue): Seq[CalledSomaticAllele] = {

    val filteredNormalPileup = PileupFilter(
      normalPileup,
      filterMultiAllelic,
      maxMappingComplexity = 100,
      minAlignmentForComplexity,
      minAlignmentQuality,
      minEdgeDistance = 0,
      maxPercentAbnormalInsertSize = 100
    )

    val filteredTumorPileup = PileupFilter(
      tumorPileup,
      filterMultiAllelic,
      maxMappingComplexity,
      minAlignmentForComplexity,
      minAlignmentQuality,
      minEdgeDistance = 0,
      maxPercentAbnormalInsertSize = 100
    )

    // For now, we skip loci that have no reads mapped. We may instead want to emit NoCall in this case.
    if (filteredTumorPileup.elements.isEmpty
      || filteredNormalPileup.elements.isEmpty
      || filteredTumorPileup.referenceDepth == filteredTumorPileup.depth // skip computation if no alternate reads
      || filteredTumorPileup.depth > maxReadDepth // skip abnormally deep pileups
      || filteredNormalPileup.depth > maxReadDepth)
      return Seq.empty

    /**
     * Find the most likely genotype in the tumor sample
     * This is either the reference genotype or an heterozygous genotype with some alternate base
     */
    val referenceAllele = Allele(Seq(filteredTumorPileup.referenceBase), Seq(filteredTumorPileup.referenceBase))
    val lodScores =
      filteredTumorPileup
        .possibleAlleles
        .filter(_.isVariant)
        .filter(allele => allele.altBases.size == 1 && allele.refBases.size == 1)
        .map(allele => {
          val variantAlleleFrequency = filteredTumorPileup.elements.count(_.allele == allele).toFloat / filteredTumorPileup.depth
          val genotype = Genotype(referenceAllele, allele)
          (
            allele, // allele
            genotype.logLikelihoodOfReads(
              filteredTumorPileup.elements,
              variantAlleleFrequency,
              includeAlignmentLikelihood = false
            ), // VAF = f tumor likelihood
              genotype.logLikelihoodOfReads(
                filteredTumorPileup.elements,
                0,
                includeAlignmentLikelihood = false
              ), // VAF = 0 tumor likelihood
                variantAlleleFrequency
          )
        })

    // LOD(T)_vaf=f - LOD(T)_vaf=0  = T
    // Keep variants where T = LOD(T)_vaf=f - LOD(T)_vaf=0 > tumorLogOdds
    val possibleSomaticAlleles = lodScores.filter(t3 => t3._2 - t3._3 > tumorLogOdds / 100.0)

    val possibleSomaticAllelesGermline = possibleSomaticAlleles.map(alleleLOD => {
      val genotype = Genotype(referenceAllele, alleleLOD._1)
      (
        alleleLOD._1, // allele
        alleleLOD._2, // VAF = f tumor likelihood
        alleleLOD._3, // VAF = 0 tumor likelihood
        // VAF = 0 normal likelihood
        genotype.logLikelihoodOfReads(
          filteredNormalPileup.elements,
          0,
          includeAlignmentLikelihood = false
        ),
          // VAF = 0.5 normal likelihood
          genotype.logLikelihoodOfReads(
            filteredNormalPileup.elements,
            0.5,
            includeAlignmentLikelihood = false
          ))
    })

    // LOD(N)_vaf=0 - LOD(N)_vaf=0.5  = G
    // Keep variants where G = LOD(N)_vaf=0 - LOD(N)_vaf=0.5 > normalLogOdds
    val filteredPossibleSomaticAllelesGermline = possibleSomaticAllelesGermline.filter(s => s._4 - s._5 > normalLogOdds / 100.0)

    //  println(filteredPossibleSomaticAllelesGermline.size)

    /**
     * Find the most likely genotype in the tumor sample
     * This is either the reference genotype or an heterozygous genotype with some alternate base
     */
    lazy val tumorLikelihoods =
      filteredTumorPileup.computeLikelihoods(
        includeAlignmentLikelihood = true,
        normalize = true
      )

    // The following lazy vals are only evaluated if mostLikelyTumorGenotype.hasVariantAllele
    lazy val normalLikelihoods =
      filteredNormalPileup.computeLikelihoods(
        includeAlignmentLikelihood = true,
        normalize = true
      )

    // NOTE(ryan): for now, compare non-reference alleles found in tumor to the sum of all likelihoods of variant
    // genotypes in the normal sample.
    // TODO(ryan): in the future, we may want to pay closer attention to the likelihood of the most likely tumor
    // genotype in the normal sample.
    for {
      alleleScores <- filteredPossibleSomaticAllelesGermline.headOption.toSeq
      allele: Allele = alleleScores._1
      genotype: Genotype = Genotype(referenceAllele, allele)
      tumorVariantLikelihood = tumorLikelihoods.filter(_._1.hasVariantAllele).map(_._2).sum
      normalVariantLikelihood = normalLikelihoods.filter(_._1.hasVariantAllele).map(_._2).sum
      tumorEvidence: AlleleEvidence = AlleleEvidence(math.exp(alleleScores._2), allele, filteredTumorPileup)
      normalEvidence = AlleleEvidence(math.exp(alleleScores._4), allele, filteredNormalPileup)
    } yield {

      CalledSomaticAllele(
        tumorPileup.sampleName,
        tumorPileup.referenceName,
        tumorPileup.locus,
        allele,
        alleleScores._2 - alleleScores._3,
        tumorEvidence,
        normalEvidence
      )
    }
  }
}
