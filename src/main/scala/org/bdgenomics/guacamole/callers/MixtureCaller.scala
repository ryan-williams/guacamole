package org.bdgenomics.guacamole.callers

import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.{ Kryo, Serializer, io }
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.cli.Args4j
import org.bdgenomics.guacamole.Common.Arguments.{ Output, TumorNormalReads }
import org.bdgenomics.guacamole._
import org.bdgenomics.guacamole.filters.PileupFilter.PileupFilterArguments
import org.bdgenomics.guacamole.filters.SomaticGenotypeFilter.SomaticGenotypeFilterArguments
import org.bdgenomics.guacamole.pileup.Pileup
import org.bdgenomics.guacamole.reads.{ MappedRead, Read }
import org.kohsuke.args4j.{ Option => Opt }

case class VariantLocus(locus: Long, variantAlleleFrequency: Double)

class VariantLocusSerializer extends Serializer[VariantLocus] {
  override def write(kryo: Kryo, output: io.Output, obj: VariantLocus) = {
    output.writeLong(obj.locus, true)
    output.writeDouble(obj.variantAlleleFrequency)
  }

  override def read(kryo: Kryo, input: Input, clazz: Class[VariantLocus]): VariantLocus = {
    val locus = input.readLong(true)
    val vaf = input.readDouble()

    VariantLocus(locus, vaf)

  }
}

object MixtureCaller extends Command with Serializable with Logging {
  override val name = "somatic-mixture"
  override val description = "cluster variant allele frequencies"

  private class Arguments
      extends DistributedUtil.Arguments
      with Output
      with SomaticGenotypeFilterArguments
      with PileupFilterArguments
      with TumorNormalReads {

    @Opt(name = "--readsToSample", usage = "Minimum log odds threshold for possible variant candidates")
    var tumorLogOdds: Int = 20

  }

  override def run(rawArgs: Array[String]): Unit = {

    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(appName = Some(name))

    val filters = Read.InputFilters(mapped = true, nonDuplicate = true, passedVendorQualityChecks = true)
    val (tumorReads, normalReads) = Common.loadTumorNormalReadsFromArguments(args, sc, filters)

    assert(tumorReads.sequenceDictionary == normalReads.sequenceDictionary,
      "Tumor and normal samples have different sequence dictionaries. Tumor dictionary: %s.\nNormal dictionary: %s."
        .format(tumorReads.sequenceDictionary, normalReads.sequenceDictionary))

    val loci = Common.loci(args, normalReads)
    val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(
      args,
      loci,
      tumorReads.mappedReads,
      normalReads.mappedReads

    )

    val normalVAFs = generateVariantLociFromReads(normalReads.mappedReads, lociPartitions)
    val tumorVAFs = generateVariantLociFromReads(tumorReads.mappedReads, lociPartitions)

  }

  def generateVariantLociFromReads(reads: RDD[MappedRead],
                                   lociPartitions: LociMap[Long],
                                   readsToSample: Int = 1000): RDD[VariantLocus] = {
    val sampleName = reads.take(1)(0).sampleName

    val variantLoci = DistributedUtil.pileupFlatMap[VariantLocus](
      reads,
      lociPartitions,
      skipEmpty = true,
      pileup => generateVariantLocus(pileup).iterator
    )
    val numVariantLoci = variantLoci.count

    Common.progress("%d non-zero variant loci in sample %s".format(numVariantLoci, sampleName))

    val sampledVAFs =
      if (numVariantLoci > readsToSample)
        variantLoci
          .sample(withReplacement = false, fraction = readsToSample.toFloat / numVariantLoci)
          .collect()
      else
        variantLoci.collect()

    val stats = new DescriptiveStatistics()
    sampledVAFs.foreach(v => stats.addValue(v.variantAlleleFrequency))

    Common.progress("Variant loci stats (min: %d, max: %d, median: %d, mean: %d, 25% : %d, 75%: %d)".format(
      stats.getMin, stats.getMax, stats.getPercentile(50), stats.getMean, stats.getPercentile(25), stats.getPercentile(75)
    ))

    variantLoci
  }

  def generateVariantLocus(pileup: Pileup): Option[VariantLocus] = {
    if (pileup.referenceDepth != pileup.depth) {
      Some(VariantLocus(pileup.locus, (pileup.depth - pileup.referenceDepth).toFloat / pileup.depth))
    } else {
      None
    }
  }
}

