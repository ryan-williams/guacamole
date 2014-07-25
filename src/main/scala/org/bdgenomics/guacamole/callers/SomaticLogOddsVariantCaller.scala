package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole._
import org.apache.spark.Logging
import org.bdgenomics.guacamole.Common.Arguments.{ TumorNormalReads, Output }
import org.kohsuke.args4j.{ Option => Opt }
import org.bdgenomics.adam.cli.Args4j
import org.apache.spark.rdd.RDD
import org.bdgenomics.guacamole.concordance.GenotypesEvaluator
import org.bdgenomics.guacamole.concordance.GenotypesEvaluator.GenotypeConcordance
import org.bdgenomics.guacamole.pileup.Pileup
import scala.collection.JavaConversions
import org.bdgenomics.adam.util.PhredUtils
import org.bdgenomics.guacamole.filters.GenotypeFilter.GenotypeFilterArguments
import org.bdgenomics.guacamole.filters.PileupFilter.PileupFilterArguments
import org.bdgenomics.guacamole.filters.{ FishersExactTest, PileupFilter, GenotypeFilter }
import org.bdgenomics.formats.avro.{ ADAMVariant, ADAMContig, ADAMGenotype }

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
    val minAlternateReadDepth = args.minAlternateReadDepth

    val lowStrandBiasLimit = args.lowStrandBiasLimit
    val highStrandBiasLimit = args.highStrandBiasLimit

    val maxNormalAlternateReadDepth = args.maxNormalAlternateReadDepth

    val oddsThreshold = args.logOdds

    val maxMappingComplexity = args.maxMappingComplexity
    val minAlignmentForComplexity = args.minAlignmentForComplexity
    val filterDeletionOverlap = args.filterDeletionOverlap

    val filterAmbiguousMapped = args.filterAmbiguousMapped
    val filterMultiAllelic = args.filterMultiAllelic
    val minAlignmentQuality = args.minAlignmentQuality
    val minLikelihood = args.minLikelihood
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
        minLikelihood,
        oddsThreshold,
        minAlignmentQuality,
        lowStrandBiasLimit,
        highStrandBiasLimit,
        maxNormalAlternateReadDepth,
        maxMappingComplexity,
        minAlignmentForComplexity,
        filterAmbiguousMapped,
        filterMultiAllelic,
        filterDeletionOverlap,
        minReadDepth,
        minAlternateReadDepth,
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
                                 minLikelihood: Int,
                                 logOddsThreshold: Int,
                                 minAlignmentQuality: Int,
                                 lowStrandBiasLimit: Int,
                                 highStrandBiasLimit: Int,
                                 maxNormalAlternateReadDepth: Int,
                                 maxMappingComplexity: Int,
                                 minAlignmentForComplexity: Int,
                                 filterAmbiguousMapped: Boolean,
                                 filterMultiAllelic: Boolean,
                                 filterDeletionOverlap: Boolean = false,
                                 minReadDepth: Int,
                                 minAlternateReadDepth: Int,
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
    if (filteredTumorPileup.elements.isEmpty || filteredNormalPileup.elements.isEmpty) // || filteredNormalPileup.depth < minReadDepth)
      return Seq.empty

    val referenceBase = Bases.baseToString(normalPileup.referenceBase)
    def normalPrior(gt: Genotype, hetVariantPrior: Double = 1e-4): Double = {
      val numberVariants = gt.numberOfVariants(referenceBase)
      if (numberVariants > 0) math.pow(hetVariantPrior / gt.uniqueAllelesCount, numberVariants) else 1
    }

    val tumorSampleName = tumorPileup.elements(0).read.sampleName
    val tumorLikelihoods =
      BayesianQualityVariantCaller.computeLikelihoods(filteredTumorPileup,
        includeAlignmentLikelihood = true,
        normalize = true).toMap

    val mapTumorLikelihoods = BayesianQualityVariantCaller.normalize(
      tumorLikelihoods.map(genotypeLikelihood =>
        (genotypeLikelihood._1, genotypeLikelihood._2 * normalPrior(genotypeLikelihood._1))))

    val tumorMostLikelyGenotype = mapTumorLikelihoods.maxBy(_._2)

    if (tumorMostLikelyGenotype._1.isVariant(referenceBase)) {
      val alternateBase = tumorMostLikelyGenotype._1.getNonReferenceAlleles(referenceBase)(0)

      // Filter deletions
      if (alternateBase.equals("")) return Seq.empty

      if (passGenotype(filteredTumorPileup,
        tumorMostLikelyGenotype._1,
        tumorMostLikelyGenotype._2,
        referenceBase,
        alternateBase,
        minLikelihood,
        minReadDepth,
        minAlternateReadDepth,
        Some(lowStrandBiasLimit))) {
        val normalLikelihoods =
          BayesianQualityVariantCaller.computeLikelihoods(filteredNormalPileup,
            includeAlignmentLikelihood = false,
            normalize = true).toMap

        val mapNormalLikelihoods = BayesianQualityVariantCaller.normalize(normalLikelihoods
          .filter(
            genotypeLikelihood => passGenotype(filteredNormalPileup,
              genotypeLikelihood._1,
              genotypeLikelihood._2,
              referenceBase,
              alternateBase,
              0,
              minReadDepth,
              1,
              None,
              Some(maxNormalAlternateReadDepth)))
          .map(genotypeLikelihood => (genotypeLikelihood._1, genotypeLikelihood._2 * normalPrior(genotypeLikelihood._1))))

        val (normalVariantGenotypes, normalReferenceGenotype) = mapNormalLikelihoods.partition(_._1.isVariant(referenceBase))

        val somaticLogOdds = math.log(tumorMostLikelyGenotype._2) - math.log(normalVariantGenotypes.map(_._2).sum)
        val normalReferenceLikelihood = normalReferenceGenotype.map(_._2).sum

        val somaticVariantProbability = tumorMostLikelyGenotype._2 * normalReferenceLikelihood
        val phredScaledSomaticLikelihood = PhredUtils.successProbabilityToPhred(somaticVariantProbability - 1e-10)
        val phredScaledGenotypeLikelihood = PhredUtils.successProbabilityToPhred(tumorMostLikelyGenotype._2 - 1e-10)
        if (somaticLogOdds.isInfinite || phredScaledSomaticLikelihood >= logOddsThreshold) {
          return buildVariants(
            tumorSampleName,
            normalPileup.referenceName,
            referenceBase,
            normalPileup.locus,
            tumorMostLikelyGenotype._1,
            tumorMostLikelyGenotype._2,
            filteredTumorPileup.depth, 0, 0)
        }
      }
    }

    return Seq.empty[ADAMGenotype]
  }

  def buildVariants(sampleName: String,
                    referenceName: String,
                    referenceBase: String,
                    locus: Long,
                    genotype: Genotype,
                    probability: Double,
                    readDepth: Int,
                    alternateReadDepth: Int,
                    alternateForwardDepth: Int,
                    delta: Double = 1e-10): Seq[ADAMGenotype] = {
    val genotypeAlleles = JavaConversions.seqAsJavaList(genotype.getGenotypeAlleles(referenceBase))
    genotype.getNonReferenceAlleles(referenceBase).map(
      variantAllele => {
        val variant = ADAMVariant.newBuilder
          .setPosition(locus)
          .setReferenceAllele(referenceBase)
          .setVariantAllele(variantAllele)
          .setContig(ADAMContig.newBuilder.setContigName(referenceName).build)
          .build
        ADAMGenotype.newBuilder
          .setAlleles(genotypeAlleles)
          .setGenotypeQuality(PhredUtils.successProbabilityToPhred(probability - delta))
          .setReadDepth(readDepth)
          .setExpectedAlleleDosage(alternateReadDepth.toFloat / readDepth)
          .setSampleId(sampleName.toCharArray)
          .setAlternateReadDepth(alternateReadDepth)
          .setVariant(variant)
          .build
      })
  }

  //  val GATKStrandBiasScore = GATKStrandBias(referenceForwardReadDepth,
  //    alternateForwardReadDepth,
  //    referenceReadDepth - referenceForwardReadDepth,
  //    alternateReadDepth - alternateForwardReadDepth)
  //

  def passGenotype(pileup: Pileup,
                   genotype: Genotype,
                   likelihood: Double,
                   referenceBase: String,
                   alternateBase: String,
                   minLikelihood: Int = 0,
                   minReadDepth: Int = 0,
                   minAlternateReadDepth: Int = 1,
                   strandBiasThreshold: Option[Int] = None,
                   maxAlternateReadDepth: Option[Int] = None): Boolean = {

    val (alternateReadDepth, alternateForwardReadDepth) = computeDepthAndForwardDepth(alternateBase, pileup)
    val (referenceReadDepth, referenceForwardReadDepth) = computeDepthAndForwardDepth(referenceBase, pileup)

    //    if (PhredUtils.successProbabilityToPhred(likelihood - 1e-10) < minLikelihood) return false

    val isCorrectAndMinimalAlternate = !genotype.isVariant(referenceBase) ||
      (genotype.alleles.toSet.contains(alternateBase) && alternateReadDepth > minAlternateReadDepth)

    if (!isCorrectAndMinimalAlternate) return false

    val strandBiasScore = BasicStrandBias(referenceForwardReadDepth,
      alternateForwardReadDepth,
      referenceReadDepth - referenceForwardReadDepth,
      alternateReadDepth - alternateForwardReadDepth)

    val fisherStrandBiasScore = fisherStrandBiasFilter(alternateReadDepth,
      alternateForwardReadDepth,
      referenceReadDepth,
      referenceForwardReadDepth)

    if (strandBiasThreshold.isDefined && (strandBiasScore * 100 > strandBiasThreshold.get) && fisherStrandBiasScore > 0.95) return false

    return true
  }

  def GATKStrandBias(a: Int, b: Int, c: Int, d: Int): Double = {
    math.max(
      (b.toFloat * c.toFloat),
      (d.toFloat * a.toFloat)) / (((c + d) * (a + b)) * ((a + b).toFloat / (a + b + c + d)))

  }

  def BasicStrandBias(a: Int, b: Int, c: Int, d: Int): Double = {
    val p1 = if ((a + b) > 0) b.toFloat / (a + b) else 0
    val p2 = if ((c + d) > 0) d.toFloat / (c + d) else 0
    val num = math.abs(p1 - p2)
    val denom = (b + d).toFloat / (a + b + c + d)
    num / denom
  }

  def fisherStrandBiasFilter(alternateReadDepth: Int,
                             alternateForwardReadDepth: Int,
                             referenceReadDepth: Int,
                             referenceForwardReadDepth: Int): Double = {
    val altEquallyForward = FishersExactTest(alternateReadDepth,
      referenceReadDepth,
      alternateForwardReadDepth,
      referenceForwardReadDepth)
    1 - altEquallyForward
    //PhredUtils.errorProbabilityToPhred(altEquallyForward)
  }

  //  def mapEstimateOfP(pileup: Pileup,
  //                     referenceAllele: String,
  //                     betaPriorAlpha: Double = 10,
  //                     betaPriorBeta: Double = 1,
  //                     includeAlignmentLikelihood: Boolean = false): Double = {
  //
  //    def computeBaseLikelihood(element: PileupElement, referenceAllele: String): Double = {
  //      val baseCallProbability = PhredUtils.phredToErrorProbability(element.qualityScore)
  //      val errorProbability = if (includeAlignmentLikelihood) {
  //        baseCallProbability + PhredUtils.phredToErrorProbability(element.read.alignmentQuality)
  //      } else {
  //        baseCallProbability
  //      }
  //
  //      if (Bases.basesToString(element.sequencedBases) == referenceAllele) 1 - errorProbability else errorProbability
  //    }
  //
  //    val estimatedPos = pileup.elements.map(el => computeBaseLikelihood(el, referenceAllele)).sum
  //
  //    (estimatedPos + betaPriorAlpha - 1) / (pileup.depth + betaPriorAlpha + betaPriorBeta)
  //  }

  def computeDepthAndForwardDepth(base: String, filteredTumorPileup: Pileup): (Int, Int) = {
    val baseElements = filteredTumorPileup.elements.filter(el => Bases.basesToString(el.sequencedBases) == base)

    val readDepth = baseElements.length
    val baseForwardReadDepth = baseElements.filter(_.read.isPositiveStrand).length
    return (readDepth, baseForwardReadDepth)
  }

  def logistic(x: Double): Double = {
    1.0 / (1 + math.exp(-x))
  }

  def scaleratio(x: Double): Double = {
    1 - (1.0 / math.abs(x))
  }

  def gompertz(x: Double, c: Double = -0.5, a: Double = 1, b: Double = -1): Double = {
    math.exp(-math.exp(c * x))
  }

}

