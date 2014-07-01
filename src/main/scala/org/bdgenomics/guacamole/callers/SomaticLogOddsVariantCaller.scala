package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole._
import org.apache.spark.Logging
import org.bdgenomics.guacamole.Common.Arguments.{ TumorNormalReads, Output }
import org.kohsuke.args4j.{ Option => Opt }
import org.bdgenomics.adam.cli.Args4j
import org.bdgenomics.adam.avro.{ ADAMContig, ADAMVariant, ADAMGenotype }
import org.apache.spark.rdd.RDD
import org.bdgenomics.guacamole.concordance.GenotypesEvaluator
import org.bdgenomics.guacamole.concordance.GenotypesEvaluator.GenotypeConcordance
import org.bdgenomics.guacamole.pileup.Pileup
import scala.collection.JavaConversions
import org.bdgenomics.adam.util.PhredUtils
import org.bdgenomics.guacamole.filters.GenotypeFilter.GenotypeFilterArguments
import org.bdgenomics.guacamole.filters.PileupFilter.PileupFilterArguments
import org.bdgenomics.guacamole.filters.{ PileupFilter, GenotypeFilter }

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

  private class Arguments extends DistributedUtil.Arguments with Output with GenotypeConcordance with GenotypeFilterArguments with PileupFilterArguments with TumorNormalReads {
    @Opt(name = "-log-odds", metaVar = "X", usage = "Make a call if the probability of variant is greater than this value (Phred-scaled)")
    var logOdds: Int = 35

  }

  override def run(rawArgs: Array[String]): Unit = {

    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(args, appName = Some(name))

    val filters = Read.InputFilters(mapped = true, nonDuplicate = true, hasMdTag = true, passedVendorQualityChecks = true)
    val (tumorReads, normalReads) = Common.loadTumorNormalReadsFromArguments(args, sc, filters)

    assert(tumorReads.sequenceDictionary == normalReads.sequenceDictionary,
      "Tumor and normal samples have different sequence dictionaries. Tumor dictionary: %s.\nNormal dictionary: %s."
        .format(tumorReads.sequenceDictionary, normalReads.sequenceDictionary))

    val minReadDepth = args.minReadDepth

    val maxAltReadDepthBias = args.maxStrandBiasAltReadDepth
    val lowStrandBiasLimit = args.lowStrandBiasLimit
    val highStrandBiasLimit = args.highStrandBiasLimit

    val oddsThreshold = args.logOdds

    val maxMappingComplexity = args.maxMappingComplexity
    val minAlignmentForComplexity = args.minAlignmentForComplexity
    val filterDeletionOverlap = args.filterDeletionOverlap

    val filterAmbiguousMapped = args.filterAmbiguousMapped
    val filterMultiAllelic = args.filterMultiAllelic
    val minAlignmentQuality = args.minAlignmentQuality

    val maxPercentAbnormalInsertSize = args.maxPercentAbnormalInsertSize

    val loci = Common.loci(args, normalReads)
    val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, loci, tumorReads.mappedReads, normalReads.mappedReads)

    val genotypes: RDD[ADAMGenotype] = DistributedUtil.pileupFlatMapTwoRDDs[ADAMGenotype](
      tumorReads.mappedReads,
      normalReads.mappedReads,
      lociPartitions,
      true, // skip empty pileups
      (pileupTumor, pileupNormal) => callSomaticVariantsAtLocus(
        pileupTumor,
        pileupNormal,
        oddsThreshold,
        minAlignmentQuality,
        lowStrandBiasLimit,
        highStrandBiasLimit,
        maxAltReadDepthBias,
        maxMappingComplexity,
        minAlignmentForComplexity,
        filterAmbiguousMapped,
        filterMultiAllelic,
        filterDeletionOverlap,
        minReadDepth,
        maxPercentAbnormalInsertSize).iterator)

    genotypes.persist()
    val filteredGenotypes = GenotypeFilter(genotypes, args)
    Common.progress("Computed %,d genotypes".format(filteredGenotypes.count))

    Common.writeVariantsFromArguments(args, filteredGenotypes)
    if (args.truthGenotypesFile != "")
      GenotypesEvaluator.printGenotypeConcordance(args, filteredGenotypes, sc)

    DelayedMessages.default.print()
  }

  /**
   *
   * Computes the genotype and probability at a given locus
   *
   * @param tumorPileup
   * @param normalPileup
   * @param logOddsThreshold
   * @param minAlignmentQuality
   * @param maxMappingComplexity
   * @param filterAmbiguousMapped
   * @param filterMultiAllelic
   * @return Sequence of possible called genotypes for all samples
   */
  def callSomaticVariantsAtLocus(tumorPileup: Pileup,
                                 normalPileup: Pileup,
                                 logOddsThreshold: Int,
                                 minAlignmentQuality: Int,
                                 lowStrandBiasLimit: Int,
                                 highStrandBiasLimit: Int,
                                 maxAltReadDepthBias: Int,
                                 maxMappingComplexity: Int,
                                 minAlignmentForComplexity: Int,
                                 filterAmbiguousMapped: Boolean,
                                 filterMultiAllelic: Boolean,
                                 filterDeletionOverlap: Boolean = false,
                                 minReadDepth: Int,
                                 maxPercentAbnormalInsertSize: Int): Seq[ADAMGenotype] = {

    val filteredNormalPileup = PileupFilter(normalPileup,
      filterAmbiguousMapped,
      filterMultiAllelic,
      maxMappingComplexity,
      minAlignmentForComplexity,
      minAlignmentQuality,
      maxPercentAbnormalInsertSize,
      filterDeletionOverlap)

    val filteredTumorPileup = PileupFilter(tumorPileup,
      filterAmbiguousMapped,
      filterMultiAllelic,
      maxMappingComplexity,
      minAlignmentForComplexity,
      minAlignmentQuality,
      maxPercentAbnormalInsertSize,
      filterDeletionOverlap)

    // For now, we skip loci that have no reads mapped. We may instead want to emit NoCall in this case.
    if (filteredTumorPileup.elements.isEmpty || filteredNormalPileup.elements.isEmpty || filteredNormalPileup.elements.length < minReadDepth)
      return Seq.empty

    val referenceBase = Bases.baseToString(normalPileup.referenceBase)
    val tumorSampleName = tumorPileup.elements(0).read.sampleName

    val tumorLikelihoods = BayesianQualityVariantCaller.computeLikelihoods(filteredTumorPileup, includeAlignmentLikelihood = true).toMap

    def buildVariants(genotype: Genotype,
                      probability: Double,
                      readDepth: Int,
                      alternateReadDepth: Int,
                      alternateForwardDepth: Int,
                      delta: Double = 1e-10): Seq[ADAMGenotype] = {
      val genotypeAlleles = JavaConversions.seqAsJavaList(genotype.getGenotypeAlleles(referenceBase))
      genotype.getNonReferenceAlleles(referenceBase).map(
        variantAllele => {
          val variant = ADAMVariant.newBuilder
            .setPosition(normalPileup.locus)
            .setReferenceAllele(referenceBase)
            .setVariantAllele(variantAllele)
            .setContig(ADAMContig.newBuilder.setContigName(normalPileup.referenceName).build)
            .build
          ADAMGenotype.newBuilder
            .setAlleles(genotypeAlleles)
            .setGenotypeQuality(PhredUtils.successProbabilityToPhred(probability - delta))
            .setReadDepth(readDepth)
            .setExpectedAlleleDosage(alternateReadDepth.toFloat / readDepth)
            .setSampleId(tumorSampleName.toCharArray)
            .setAlternateReadDepth(alternateReadDepth)
            .setVariant(variant)
            .build
        })
    }

    val tumorMostLikelyGenotype = tumorLikelihoods.maxBy(_._2)
    if (!tumorMostLikelyGenotype._1.isVariant(referenceBase)) return Seq.empty

    val normalLikelihoods = BayesianQualityVariantCaller.computeLikelihoods(filteredNormalPileup, includeAlignmentLikelihood = true).toMap

    if (normalLikelihoods.isEmpty) return Seq.empty
    val (normalVariantGenotypes, normalReferenceGenotype) = normalLikelihoods.partition(_._1.isVariant(referenceBase))

    val normalizedTumorLikelihood = tumorMostLikelyGenotype._2 / tumorLikelihoods.map(_._2).sum
    val normalizedNormalLikelihood = normalVariantGenotypes.map(_._2).sum / normalLikelihoods.map(_._2).sum
    val somaticVariantOdds = PhredUtils.successProbabilityToPhred(normalizedTumorLikelihood - normalizedNormalLikelihood - 1e-10)

    if (somaticVariantOdds < logOddsThreshold) return Seq.empty

    val alternateBase = tumorMostLikelyGenotype._1.getNonReferenceAlleles(referenceBase)(0)
    val alternateElements = filteredTumorPileup.elements.filter(el => Bases.basesToString(el.sequencedBases) == alternateBase)
    val referenceElements = filteredTumorPileup.elements.filter(el => Bases.basesToString(el.sequencedBases) == referenceBase)

    val referenceReadDepth = referenceElements.length
    val referenceForwardReadDepth = referenceElements.filter(_.read.isPositiveStrand).length

    val alternateReadDepth = alternateElements.length
    val alternateForwardReadDepth = alternateElements.filter(_.read.isPositiveStrand).length

    //TODO to apply strand bias filter here for now
    val alternateStrandBias = 100.0 * alternateForwardReadDepth / alternateReadDepth
    val referenceStrandBias = 100.0 * referenceForwardReadDepth / referenceReadDepth
    val strandBias = 100.0 * (alternateForwardReadDepth + referenceForwardReadDepth) / (alternateReadDepth + referenceReadDepth)
    //    if (alternateForwardReadDepth < maxAltReadDepthBias && ((strandBias <= lowStrandBiasLimit && referenceStrandBias > 50) || (strandBias >= highStrandBiasLimit && referenceStrandBias < 50))) return Seq.empty

    if (math.abs(strandBias - referenceStrandBias) > maxAltReadDepthBias || math.abs(strandBias - alternateStrandBias) > maxAltReadDepthBias) return Seq.empty

    buildVariants(tumorMostLikelyGenotype._1, normalizedTumorLikelihood, filteredTumorPileup.depth, alternateReadDepth, alternateForwardReadDepth)

  }

}

