package org.bdgenomics.guacamole.commands

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.cli.Args4j
import org.apache.spark.SparkContext._
import org.bdgenomics.guacamole.reads.Read
import org.bdgenomics.guacamole.{ ReadSet, DistributedUtil, Common, Command }
import org.kohsuke.args4j.{ Argument, Option }

object ReadDepthHist extends Command with Serializable with Logging {
  override val name = "read-depth-hist"
  override val description = "Compute a histogram of read depths"

  private class Arguments
      extends DistributedUtil.Arguments {

    @Argument(metaVar = "OUTFILE", required = true, usage = "BAM file to compute histogram for.")
    var inputFile: String = null

  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(appName = Some(name))

    val readSet = ReadSet(sc, args.inputFile, Read.InputFilters.empty, 1, true)
    val reads = readSet.reads

    val unmappedReads = reads.filter(!_.isMapped)
    val mappedReads = reads.flatMap(_.getMappedReadOpt)

    val readDepthPerLocus: RDD[((String, Long), Long)] =
      mappedReads.flatMap(read => {
        (0 until read.sequence.size).map(offset =>
          ((read.referenceContig, read.start + offset), 1L)
        )
      }).reduceByKey(_ + _)

    val lociPerReadDepth: collection.Map[Long, Long] =
      readDepthPerLocus.map({
        case (locus, count) => (count, 1L)
      }).reduceByKeyLocally(_ + _)

    println("Loci per read depth:\n\n%s".format(
      lociPerReadDepth.toList.sortBy(_._1).map({
        case (depth, numLoci) => "%8d: %d".format(depth, numLoci)
      }).mkString("\t", "\n\t", "")
    ))
  }
}
