package org.hammerlab.guacamole.commands

import java.io.{BufferedWriter, FileWriter}

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.hammerlab.guacamole.distributed.PileupFlatMapUtils.pileupFlatMapMultipleRDDs
import org.hammerlab.guacamole.loci.partitioning.LociPartitioning
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.readsets.args.{Arguments => ReadSetsArguments}
import org.hammerlab.guacamole.readsets.loading.{Input, InputFilters, ReadLoadingConfig}
import org.hammerlab.guacamole.readsets.{PerSample, ReadSets}
import org.hammerlab.guacamole.reference.{ReferenceBroadcast, ReferenceGenome}
import org.kohsuke.args4j.{Option => Args4jOption}

/**
 * VariantLocus is a locus and the variant allele frequency at that locus
 *
 * @param locus Position of non-reference alleles
 * @param variantAlleleFrequency Frequency of non-reference alleles
 */
case class VariantLocus(sampleName: String, contig: String, locus: Long, variantAlleleFrequency: Float)

object VariantLocus {

  /**
   * Construct VariantLocus from a pileup
   *
   * @param pileup Pileup of reads at a given locus
   * @return VariantLocus at reference position, locus
   */
  def apply(pileup: Pileup): Option[VariantLocus] = {
    if (pileup.referenceDepth != pileup.depth) {
      val contigName = pileup.elements.head.read.contigName
      Some(
        VariantLocus(
          pileup.sampleName,
          contigName,
          pileup.locus,
          (pileup.depth - pileup.referenceDepth).toFloat / pileup.depth
        )
      )
    } else {
      None
    }
  }

}

object VAFHistogram {

  protected class Arguments extends ReadSetsArguments {

    @Args4jOption(name = "--out", required = false, forbids = Array("--local-out"),
      usage = "HDFS file path to save the variant allele frequency histogram")
    var output: String = ""

    @Args4jOption(name = "--local-out", required = false, forbids = Array("--out"),
      usage = "Local file path to save the variant allele frequency histogram")
    var localOutputPath: String = ""

    @Args4jOption(name = "--bins", required = false,
      usage = "Number of bins for the variant allele frequency histogram (Default: 20)")
    var bins: Int = 20

    @Args4jOption(name = "--cluster", required = false,
      usage = "Cluster the variant allele frequencies using a Gaussian mixture model")
    var cluster: Boolean = false

    @Args4jOption(name = "--num-clusters", required = false, depends = Array("--cluster"),
      usage = "Number of clusters for the Gaussian mixture model (Default: 3)")
    var numClusters: Int = 3

    @Args4jOption(name = "--min-read-depth", usage = "Minimum read depth to include variant allele frequency")
    var minReadDepth: Int = 0

    @Args4jOption(name = "--min-vaf", usage = "Minimum variant allele frequency to include")
    var minVAF: Int = 0

    @Args4jOption(name = "--print-stats", usage = "Print descriptive statistics on variant allele frequncy distribution")
    var printStats: Boolean = false

    @Args4jOption(name = "--sample-percent", depends = Array("--print-stats"),
      usage = "Percent of variant to use for the calculations (Default: 25)")
    var samplePercent: Int = 25

    @Args4jOption(name = "--reference-fasta", required = true, usage = "Local path to a reference FASTA file")
    var referenceFastaPath: String = ""
  }

  object Caller extends SparkCommand[Arguments] {
    override val name = "vaf-histogram"
    override val description = "Compute and cluster the variant allele frequencies"

    override def run(args: Arguments, sc: SparkContext): Unit = {
      val loci = args.parseLoci(sc.hadoopConfiguration)
      val filters =
        InputFilters(
          overlapsLoci = loci,
          nonDuplicate = true,
          passedVendorQualityChecks = true
        )

      val samplePercent = args.samplePercent

      val readsets =
        ReadSets(
          sc,
          args.inputs,
          filters,
          contigLengthsFromDictionary = true,
          config = ReadLoadingConfig(args)
        )

      val ReadSets(readsRDDs, _, contigLengths) = readsets

      val lociPartitions =
        args
          .getPartitioner(readsets.allMappedReads)
          .partition(loci.result(contigLengths))

      val reference = ReferenceBroadcast(args.referenceFastaPath, sc)

      val minReadDepth = args.minReadDepth
      val minVariantAlleleFrequency = args.minVAF

      val variantLoci =
        variantLociFromReads(
          readsets.sampleNames,
          readsets.mappedReadsRDDs,
          reference,
          lociPartitions,
          samplePercent,
          minReadDepth,
          minVariantAlleleFrequency,
          printStats = args.printStats
        )

      val bins = args.bins

      val variantAlleleHistograms = generateVAFHistogram(variantLoci, bins)

      val binSize = 100 / bins

      def histogramToString(kv: (Int, Long)): String = {
        s"${kv._1}, ${math.min(kv._1 + binSize, 100)}, ${kv._2}"
      }

      val histogramOutput =
        (for {
          Input(sampleName, filename) <- args.inputs
          histogram = variantAlleleHistograms(sampleName)
        } yield {
          histogram
            .toSeq
            .sortBy(_._1)
            .map(kv => s"$filename, $sampleName, ${histogramToString(kv)}")
        }).flatten

      if (args.localOutputPath != "") {
        val writer = new BufferedWriter(new FileWriter(args.localOutputPath))
        writer.write("Filename, SampleName, BinStart, BinEnd, Size")
        writer.newLine()
        histogramOutput.foreach(line => {
          writer.write(line)
          writer.newLine()
        })
        writer.flush()
        writer.close()
      } else if (args.output != "") {
        // Parallelize histogram and save on HDFS
        sc.parallelize(histogramOutput).saveAsTextFile(args.output)
      } else {
        // Print histograms to standard out
        for {
          (_, histogram) <- variantAlleleHistograms
        } {
          histogram.toSeq.sortBy(_._1).foreach(kv => println(histogramToString(kv)))
        }
      }

      if (args.cluster) {
        buildMixtureModel(variantLoci, args.numClusters)
      }

    }
  }

  /**
   * Generates a count of loci in each variant allele frequency bins
   *
   * @param variantAlleleFrequencies RDD of loci with variant allele frequency > 0
   * @param bins Number of bins to group the VAFs into
   * @return Map of rounded variant allele frequency to number of loci with that value
   */
  def generateVAFHistogram(variantAlleleFrequencies: RDD[VariantLocus], bins: Int): Map[String, Map[Int, Long]] = {
    assume(bins <= 100 && bins >= 1, "Bins should be between 1 and 100")

    def roundToBin(variantAlleleFrequency: Float) = {
      val variantPercent = (variantAlleleFrequency * 100).toInt
      variantPercent - (variantPercent % (100 / bins))
    }

    variantAlleleFrequencies
      .map(vaf => (vaf.sampleName, roundToBin(vaf.variantAlleleFrequency)) -> 1L)
      .reduceByKey(_ + _)
      .map(t => t._1._1 -> Map(t._1._2 -> t._2))
      .reduceByKey(_ ++ _)
      .collectAsMap
      .toMap
  }

  /**
   * Find all non-reference loci in the sample
   *
   * @param readsRDDs RDD of mapped reads for all samples
   * @param reference genome
   * @param lociPartitions Positions which to examine for non-reference loci
   * @param samplePercent Percent of non-reference loci to use for descriptive statistics
   * @param minReadDepth Minimum read depth before including variant allele frequency
   * @param minVariantAlleleFrequency Minimum variant allele frequency to include
   * @param printStats Print descriptive statistics for the variant allele frequency distribution
   * @return RDD of VariantLocus, which contain the locus and non-zero variant allele frequency
   */
  def variantLociFromReads(sampleNames: PerSample[String],
                           readsRDDs: PerSample[RDD[MappedRead]],
                           reference: ReferenceGenome,
                           lociPartitions: LociPartitioning,
                           samplePercent: Int = 100,
                           minReadDepth: Int = 0,
                           minVariantAlleleFrequency: Int = 0,
                           printStats: Boolean = false): RDD[VariantLocus] = {
    val variantLoci =
      pileupFlatMapMultipleRDDs[VariantLocus](
        sampleNames,
        readsRDDs,
        lociPartitions,
        skipEmpty = true,
        pileups =>
          pileups
            .iterator
            .flatMap(pileup =>
              VariantLocus(pileup)
                .filter(locus => pileup.depth >= minReadDepth)
                .filter(_.variantAlleleFrequency >= (minVariantAlleleFrequency / 100.0))
            ),
        reference
      )

    if (printStats) {
      variantLoci.persist(StorageLevel.MEMORY_ONLY)

      val numVariantLociBySample = variantLoci.map(_.sampleName -> 1L).reduceByKey(_ + _).collect()
      progress(
        "non-zero variant loci per-sample:",
        numVariantLociBySample.map(t => s"${t._1}:\t${t._2}").mkString("\n")
      )

      val sampleNames = numVariantLociBySample.map(_._1)

      // Sample variant loci to compute descriptive statistics
      val sampledVAFs =
        if (samplePercent < 100)
          variantLoci
            .keyBy(_.sampleName)
            .sampleByKey(withReplacement = false, fractions = sampleNames.map(_ -> samplePercent / 100.0).toMap)
            .groupByKey()
            .collect()
        else
          variantLoci.groupBy(_.sampleName).collect()

      for {
        (sampleName, vafs) <- sampledVAFs
      } {
        val stats = new DescriptiveStatistics()
        vafs.foreach(v => stats.addValue(v.variantAlleleFrequency))

        // Print out descriptive statistics for the variant allele frequency distribution
        progress(
          "Variant loci stats for %s (min: %f, max: %f, median: %f, mean: %f, 25Pct: %f, 75Pct: %f)".format(
            sampleName,
            stats.getMin,
            stats.getMax,
            stats.getPercentile(50),
            stats.getMean,
            stats.getPercentile(25),
            stats.getPercentile(75)
          )
        )
      }
    }

    variantLoci
  }

  /**
   * Fit a Gaussian mixture model to the distribution of variant allele frequencies
   *
   * @param variantAlleleFrequencies RDD of loci with variant allele frequency > 0
   * @param numClusters Number of Gaussian distributions to fit
   * @param maxIterations Maximum number of iterations to run EM
   * @param convergenceTol Largest change in log-likelihood before convergence
   * @return GaussianMixtureModel
   */
  def buildMixtureModel(variantAlleleFrequencies: RDD[VariantLocus],
                        numClusters: Int,
                        maxIterations: Int = 50,
                        convergenceTol: Double = 1e-2): GaussianMixtureModel = {

    val vafVectors = variantAlleleFrequencies.map(vaf => Vectors.dense(vaf.variantAlleleFrequency))

    val model =
      new GaussianMixture()
        .setK(numClusters)
        .setConvergenceTol(convergenceTol)
        .setMaxIterations(maxIterations)
        .run(vafVectors)

    for (i <- 0 until model.k) {
      println(s"Cluster $i: mean=${model.gaussians(i).mu(0)}, std. deviation=${model.gaussians(i).sigma}, weight=${model.weights(i)}")
    }

    model
  }

}

