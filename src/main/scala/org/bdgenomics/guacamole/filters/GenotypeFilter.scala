package org.bdgenomics.guacamole.filters

import org.apache.spark.rdd.RDD
import org.bdgenomics.guacamole.Common.Arguments.Base
import org.kohsuke.args4j.Option
import org.bdgenomics.guacamole.Common
import org.bdgenomics.formats.avro.ADAMGenotype

/**
 * Filter to remove genotypes where the number of reads at the locus is low
 */
object MinimumLikelihoodFilter {

  def hasMinimumLikelihood(genotype: ADAMGenotype,
                           minLikelihood: Int,
                           includeNull: Boolean = true): Boolean = {
    if (genotype.getGenotypeQuality != null) {
      genotype.getGenotypeQuality > minLikelihood
    } else {
      includeNull
    }
  }

  /**
   *
   *  Apply the filter to an RDD of genotypes
   *
   * @param genotypes RDD of genotypes to filter
   * @param minLikelihood minimum quality score for this genotype
   * @param includeNull include the genotype if the required fields are nu
   * @param debug if true, compute the count of genotypes after filtering
   * @return Genotypes with quality > minLikelihood
   */
  def apply(genotypes: RDD[ADAMGenotype],
            minLikelihood: Int,
            debug: Boolean = false,
            includeNull: Boolean = true): RDD[ADAMGenotype] = {
    val filteredGenotypes = genotypes.filter(hasMinimumLikelihood(_, minLikelihood, includeNull))
    if (debug) GenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

/**
 * Filter to remove genotypes where the number of reads at the locus is low
 */
object MinimumReadDepthFilter {

  def hasMinimumReadDepth(genotype: ADAMGenotype,
                          minReadDepth: Int,
                          includeNull: Boolean = true): Boolean = {
    if (genotype.readDepth != null) {
      genotype.readDepth >= minReadDepth
    } else {
      includeNull
    }
  }

  /**
   *
   *  Apply the filter to an RDD of genotypes
   *
   * @param genotypes RDD of genotypes to filter
   * @param minReadDepth minimum number of reads at locus for this genotype
   * @param includeNull include the genotype if the required fields are nu
   * @param debug if true, compute the count of genotypes after filtering
   * @return Genotypes with read depth > minReadDepth
   */
  def apply(genotypes: RDD[ADAMGenotype],
            minReadDepth: Int,
            debug: Boolean = false,
            includeNull: Boolean = true): RDD[ADAMGenotype] = {
    val filteredGenotypes = genotypes.filter(hasMinimumReadDepth(_, minReadDepth, includeNull))
    if (debug) GenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

/**
 * Filter to remove genotypes where the number of reads to support the alternate allele is low
 */
object MinimumAlternateReadDepthFilter {

  /**
   *
   * Apply the filter to an RDD of genotypes
   *
   * @param genotypes RDD of genotypes to filter
   * @param minAlternateReadDepth minimum number of reads with alternate allele at locus for this genotype
   * @param includeNull include the genotype if the required fields are null
   * @param debug if true, compute the count of genotypes after filtering
   * @return Genotypes with read depth > minAlternateReadDepth
   */
  def apply(genotypes: RDD[ADAMGenotype],
            minAlternateReadDepth: Int,
            debug: Boolean = false,
            includeNull: Boolean = true): RDD[ADAMGenotype] = {
    val filteredGenotypes = genotypes.filter(hasMinimumAlternateReadDepth(_, minAlternateReadDepth, includeNull))
    if (debug) GenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }

  def hasMinimumAlternateReadDepth(genotype: ADAMGenotype,
                                   minAlternateReadDepth: Int,
                                   includeNull: Boolean = true): Boolean = {
    if (genotype.alternateReadDepth != null) {
      genotype.alternateReadDepth >= minAlternateReadDepth
    } else {
      includeNull
    }
  }
}

/**
 * Filter to remove genotypes where most of the reads that support come from a single strand
 */
object StrandBiasFilter {

  /**
   *
   * This filter will remove genotypes where most of the supporting reads come from a single strand.
   * Genotypes pass this filter by having there strand ratio between an upper and lower limit.
   * If the number of reads is high we may want to ignore this filter so this filter is only applicable up to a number
   * of reads (maxStrandBiasReadDepth)
   *
   * @param genotype Genotype to evaluate
   * @param lowStrandBiasLimit Minimum allowed percent of reads on the forward strand
   * @param highStrandBiasLimit Maximum allowed percent of reads on the forward strand
   * @return
   */
  def isBiasedToSingleStrand(genotype: ADAMGenotype,
                             lowStrandBiasLimit: Int,
                             highStrandBiasLimit: Int,
                             includeNull: Boolean = true): Boolean = {

    // TODO! fix this for current ADAM version
    throw new NotImplementedError("implement this for new ADAM")
    /*
    if (genotype.readsMappedForwardStrand != null && genotype.readDepth != null && genotype.alternateReadDepth != null) {
      val strandRatio = genotype.readsMappedForwardStrand / genotype.readDepth
      strandRatio < highStrandBiasLimit && strandRatio > lowStrandBiasLimit && genotype.alternateReadDepth < maxStrandBiasReadDepth
    } else {
      includeNull
    }
    */
    false
  }

  /**
   *  Apply the filter to an RDD of genotypes
   *
   * @param genotypes RDD of genotypes to filter
   * @param lowStrandBiasLimit Minimum allowed percent of reads on the forward strand
   * @param highStrandBiasLimit Maximum allowed percent of reads on the forward strand
   * @param includeNull include the genotype if the required fields are null
   * @param debug if true, compute the count of genotypes after filtering
   * @return Genotypes with read depth > minAlternateReadDepth
   */
  def apply(genotypes: RDD[ADAMGenotype], lowStrandBiasLimit: Int,
            highStrandBiasLimit: Int,
            maxStrandBiasReadDepth: Int,
            debug: Boolean = false,
            includeNull: Boolean = true): RDD[ADAMGenotype] = {
    val filteredGenotypes = genotypes.filter(isBiasedToSingleStrand(_, lowStrandBiasLimit, highStrandBiasLimit, includeNull))
    if (debug) GenotypeFilter.printFilterProgress(filteredGenotypes)
    filteredGenotypes
  }
}

object GenotypeFilter {

  def printFilterProgress(filteredGenotypes: RDD[ADAMGenotype]) = {
    filteredGenotypes.persist()
    Common.progress("Filtered genotypes down to %d genotypes".format(filteredGenotypes.count()))
  }

  trait GenotypeFilterArguments extends Base {

    @Option(name = "-minReadDepth", usage = "Minimum number of reads for a genotype call")
    var minReadDepth: Int = 0

    @Option(name = "-minAlternateReadDepth", usage = "Minimum number of reads with alternate allele for a genotype call")
    var minAlternateReadDepth: Int = 0

    @Option(name = "-maxNormalAlternateReadDepth", usage = "Maximum number of alternate reads in the normal sample")
    var maxNormalAlternateReadDepth: Int = 0

    @Option(name = "-strandBiasLimit", usage = "Minimum allowed % of reads on the forward strand. (To prevent strand bias)")
    var lowStrandBiasLimit: Int = 0

    @Option(name = "-debug-genotype-filters", usage = "Print count of genotypes after each filtering step")
    var debugGenotypeFilters = false

    @Option(name = "-minLikelihood", usage = "Minimum Phred-scaled likelihood. Default: 0 (off)")
    var minLikelihood: Int = 0

  }

  def apply(genotypes: RDD[ADAMGenotype], args: GenotypeFilterArguments): RDD[ADAMGenotype] = {
    var filteredGenotypes = genotypes

    if (args.minReadDepth > 0) {
      filteredGenotypes = MinimumReadDepthFilter(filteredGenotypes, args.minReadDepth, args.debugGenotypeFilters)
    }

    //    if (args.minAlternateReadDepth > 0) {
    //      filteredGenotypes = MinimumAlternateReadDepthFilter(filteredGenotypes, args.minAlternateReadDepth, args.debugGenotypeFilters)
    //    }

    //  TODO: Removed until these fields are returned to genotype
    //    if (args.lowStrandBiasLimit >= 0 || args.highStrandBiasLimit <= 100) {
    //      filteredGenotypes = StrandBiasFilter(genotypes, args.lowStrandBiasLimit, args.highStrandBiasLimit, args.maxStrandBiasAltReadDepth)
    //    }

    //    if (args.minLikelihood > 0) {
    //      filteredGenotypes = MinimumLikelihoodFilter(filteredGenotypes, args.minLikelihood, args.debugGenotypeFilters)
    //    }

    filteredGenotypes
  }

  def apply(genotypes: Seq[ADAMGenotype],
            minReadDepth: Int,
            minAlternateReadDepth: Int,
            minLikelihood: Int): Seq[ADAMGenotype] = {
    var filteredGenotypes = genotypes

    if (minReadDepth > 0) {
      filteredGenotypes = filteredGenotypes.filter(MinimumReadDepthFilter.hasMinimumReadDepth(_, minReadDepth))
    }

    if (minAlternateReadDepth > 0) {
      filteredGenotypes = filteredGenotypes.filter(MinimumAlternateReadDepthFilter.hasMinimumAlternateReadDepth(_, minAlternateReadDepth))
    }

    //  TODO: Removed until these fields are returned to genotype
    //    if (args.lowStrandBiasLimit >= 0 || args.highStrandBiasLimit <= 100) {
    //      filteredGenotypes = StrandBiasFilter(genotypes, args.lowStrandBiasLimit, args.highStrandBiasLimit, args.maxStrandBiasAltReadDepth)
    //    }

    //    if (args.minLikelihood > 0) {
    //      filteredGenotypes = MinimumLikelihoodFilter(filteredGenotypes, args.minLikelihood, args.debugGenotypeFilters)
    //    }

    filteredGenotypes
  }

}