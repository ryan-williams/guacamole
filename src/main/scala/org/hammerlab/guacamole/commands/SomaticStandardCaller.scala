package org.hammerlab.guacamole.commands

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.formats.avro.DatabaseVariantAnnotation
import org.hammerlab.guacamole.distributed.PileupFlatMapUtils.pileupFlatMapTwoSamples
import org.hammerlab.guacamole.filters.somatic.SomaticGenotypeFilter
import org.hammerlab.guacamole.filters.somatic.SomaticGenotypeFilter.SomaticGenotypeFilterArguments
import org.hammerlab.guacamole.likelihood.Likelihood
import org.hammerlab.guacamole.likelihood.Likelihood.likelihoodsOfGenotypes
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.readsets.ReadSets
import org.hammerlab.guacamole.readsets.args.{ReferenceArgs, TumorNormalReadsArgs}
import org.hammerlab.guacamole.readsets.rdd.{PartitionedRegions, PartitionedRegionsArgs}
import org.hammerlab.guacamole.variants.{Allele, AlleleEvidence, CalledSomaticAllele, Genotype, GenotypeOutputArgs, GenotypeOutputCaller}
import org.kohsuke.args4j.{Option => Args4jOption}

/**
 * Simple subtraction based somatic variant caller
 *
 * This takes two variant callers, calls variants on tumor and normal independently,
 * and outputs the variants in the tumor sample BUT NOT the normal sample.
 *
 * This assumes that both read sets only contain a single sample, otherwise we should compare
 * on a sample identifier when joining the genotypes
 *
 */
object SomaticStandard {

  class Arguments
    extends Args
      with TumorNormalReadsArgs
      with PartitionedRegionsArgs
      with SomaticGenotypeFilterArguments
      with GenotypeOutputArgs
      with ReferenceArgs {

    @Args4jOption(
      name = "--normal-odds",
      usage = "Minimum log odds threshold for possible variant candidates"
    )
    var normalOddsThreshold: Int = 5

    @Args4jOption(
      name = "--tumor-odds",
      usage = "Minimum log odds threshold for possible variant candidates"
    )
    var tumorOddsThreshold: Int = 10

    @Args4jOption(
      name = "--max-normal-alternate-read-depth",
      usage = "Maximum number of alternates in the normal sample"
    )
    var maxNormalAlternateReadDepth: Int = 4

    @Args4jOption(
      name = "--min-tumor-variant-allele-frequency",
      usage = "Minimum VAF at which to test somatic variants"
    )
    var minTumorVariantAlleleFrequency: Int = 3

    @Args4jOption(name = "--dbsnp-vcf", required = false, usage = "VCF file to identify DBSNP variants")
    var dbSnpVcf: String = ""
  }

  object Caller extends GenotypeOutputCaller[Arguments, CalledSomaticAllele] {
    override val name = "somatic-standard"
    override val description = "call somatic variants using independent callers on tumor and normal"

    override def computeVariants(args: Arguments, sc: SparkContext) = {
      val reference = args.reference(sc)

      val (readsets, loci) = ReadSets(sc, args)

      val partitionedReads =
        PartitionedRegions(
          readsets.allMappedReads,
          loci,
          args
        )

      // Destructure `args`' fields here to avoid serializing `args` itself.
      val normalOddsThreshold = args.normalOddsThreshold
      val tumorOddsThreshold = args.tumorOddsThreshold

      val maxTumorReadDepth = args.maxTumorReadDepth

      val normalSampleName = args.normalSampleName
      val tumorSampleName = args.tumorSampleName

      val maxNormalAlternateReadDepth = args.maxNormalAlternateReadDepth
      val minTumorVariantAlleleFrequency = args.minTumorVariantAlleleFrequency / 100.0f

      var potentialGenotypes: RDD[CalledSomaticAllele] =
        pileupFlatMapTwoSamples[CalledSomaticAllele](
          partitionedReads,
          sample1Name = normalSampleName,
          sample2Name = tumorSampleName,
          skipEmpty = true,  // skip empty pileups
          function = (pileupNormal, pileupTumor) =>
            findPotentialVariantAtLocus(
              pileupTumor,
              pileupNormal,
              normalOddsThreshold,
              tumorOddsThreshold,
              maxTumorReadDepth,
              maxNormalAlternateReadDepth,
              minTumorVariantAlleleFrequency

            ).iterator,
          reference = reference
        )

      potentialGenotypes.persist()
      progress("Computed %,d potential genotypes".format(potentialGenotypes.count))

      if (args.dbSnpVcf != "") {
        val adamContext = new ADAMContext(sc)
        val dbSnpVariants = adamContext.loadVariantAnnotations(args.dbSnpVcf)

        potentialGenotypes =
          potentialGenotypes
            .keyBy(_.bdgVariant)
            .leftOuterJoin(dbSnpVariants.rdd.keyBy(_.getVariant))
            .values
            .map {
              case (calledAllele: CalledSomaticAllele, dbSnpVariantOpt: Option[DatabaseVariantAnnotation]) =>
                calledAllele.copy(rsID = dbSnpVariantOpt.map(_.getDbSnpId))
            }
      }

      (
        SomaticGenotypeFilter(potentialGenotypes, args),
        readsets.sequenceDictionary,
        Vector(args.tumorSampleName)
      )
    }

    def findPotentialVariantAtLocus(tumorPileup: Pileup,
                                    normalPileup: Pileup,
                                    normalOddsThreshold: Int,
                                    tumorOddsThreshold: Int,
                                    maxReadDepth: Int = Int.MaxValue,
                                    maxNormalAlternateReadDepth: Int = 5,
                                    minTumorVariantAlleleFrequency: Float = 0.05f): Option[CalledSomaticAllele] = {

      // For now, we skip loci that have no reads mapped. We may instead want to emit NoCall in this case.
      if (tumorPileup.elements.isEmpty
        || normalPileup.elements.isEmpty
        || tumorPileup.depth > maxReadDepth // skip abnormally deep pileups
        || normalPileup.depth > maxReadDepth
        || tumorPileup.referenceDepth == tumorPileup.depth // skip computation if no alternate reads
        || normalPileup.depth - normalPileup.referenceDepth > maxNormalAlternateReadDepth
        )
        return None

      val referenceAllele = Allele(tumorPileup.referenceBase, tumorPileup.referenceBase)
      val referenceGenotype = Genotype(Map(referenceAllele -> 1.0))

      val tumorDepth = tumorPileup.depth
      val variantAlleleFractions: Map[Allele, Double] =
        tumorPileup
          .elements
          .withFilter(_.allele.isVariant)
          .map(_.allele)
          .groupBy(identity)
          .map(kv => kv._1 -> kv._2.size / tumorDepth.toDouble )

      // Compute empirical frequency of alternate allele in the tumor sample
      // for the likelihood computation
      val mostFrequentVariantAllele = variantAlleleFractions.maxBy(_._2)
      val empiricalVariantAlleleFrequency =  math.max(minTumorVariantAlleleFrequency, mostFrequentVariantAllele._2)

      // Build a possible genotype where the alternate allele occurs at the
      // observed empirical VAF
      val somaticVariantGenotype =
        Genotype(
          Map(
            referenceAllele -> (1.0 - empiricalVariantAlleleFrequency),
            mostFrequentVariantAllele._1 -> empiricalVariantAlleleFrequency
          )
        )


      val tumorLikelihoods = likelihoodsOfGenotypes(
        tumorPileup.elements,
        Array(referenceGenotype, somaticVariantGenotype),
        prior = Likelihood.uniformPrior,
        includeAlignment = false,
        logSpace = true,
        normalize = true
      )
      val tumorLOD: Double = tumorLikelihoods(1) - tumorLikelihoods(0)

      val germlineVariantGenotype =
        Genotype(
          Map(
            referenceAllele -> 0.5,
            mostFrequentVariantAllele._1 -> 0.5
          )
        )

      val normalLikelihoods = likelihoodsOfGenotypes(
        normalPileup.elements,
        Array(referenceGenotype, germlineVariantGenotype),
        prior = Likelihood.uniformPrior,
        includeAlignment = false,
        logSpace = true,
        normalize = true
      )

      val normalLOD: Double = normalLikelihoods(0) - normalLikelihoods(1)
      if (tumorLOD > tumorOddsThreshold && normalLOD > normalOddsThreshold && mostFrequentVariantAllele._1.altBases.nonEmpty) {
        val allele = mostFrequentVariantAllele._1

        val tumorVariantEvidence = AlleleEvidence(math.exp(-tumorLikelihoods(1)), allele, tumorPileup)
        val normalReferenceEvidence = AlleleEvidence(math.exp(-normalLikelihoods(0)), referenceAllele, normalPileup)
        Some(
          CalledSomaticAllele(
            tumorPileup.sampleName,
            tumorPileup.contigName,
            tumorPileup.locus,
            allele,
            tumorLOD,
            tumorVariantEvidence,
            normalReferenceEvidence
          )
        )
      } else {
        None
      }
    }
  }
}
