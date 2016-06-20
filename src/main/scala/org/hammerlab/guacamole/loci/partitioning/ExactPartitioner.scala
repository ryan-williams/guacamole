package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.map.LociMap
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.PartitionIndex
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.readsets.RegionRDD
import org.hammerlab.guacamole.readsets.RegionRDD._
import org.hammerlab.guacamole.reference.{Contig, ReferenceRegion}
import org.hammerlab.guacamole.util.KeyOrdering
import org.hammerlab.magic.iterator.GroupRunsIterator
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.reflect.ClassTag

trait ExactPartitionerArgs extends LociPartitionerArgs {
  @Args4jOption(
    name = "--max-reads-per-partition",
    usage = "Maximum number of reads to allow any one partition to have. Loci that have more depth than this will be dropped."
  )
  var maxReadsPerPartition: Int = 500000
}

class ExactPartitioner[R <: ReferenceRegion: ClassTag](regions: RDD[R],
                                                       halfWindowSize: Int,
                                                       maxRegionsPerPartition: Int)
  extends LociPartitioner {

  def partition(loci: LociSet): LociPartitioning = {
    val lociSetsRDD = regions.makeCappedLociSets(halfWindowSize, loci, maxRegionsPerPartition)

    val depthRunsRDD = regions.partitionDepths(halfWindowSize, loci, maxRegionsPerPartition)
    val (validLoci, invalidLoci) = RegionRDD.validLociCounts(depthRunsRDD)

    val totalLoci = validLoci + invalidLoci

    val numDepthRuns = depthRunsRDD.count
    val numDepthRunsToTake = 1000
    val depthRuns =
      if (numDepthRuns <= numDepthRunsToTake)
        depthRunsRDD.collect()
      else
        depthRunsRDD.take(numDepthRunsToTake)

    val avgRunLength =
      (for { (_, num) <- depthRuns } yield (num * num).toLong).sum.toDouble / validLoci

    val depthRunsByContig =
      depthRuns
        .groupBy(_._1._1)
        .mapValues(_.map {
          case ((_, valid), num) => num -> valid
        })
        .toArray
        .sorted(new KeyOrdering[Contig, Array[(Int, Boolean)]](Contig.ordering))

    val overflowMsg =
      if (numDepthRuns > numDepthRunsToTake)
        s". First $numDepthRunsToTake runs:"
      else
        ":"

    def runsStr(runsIter: Iterator[(Int, Boolean)]): String = {
      val runs = runsIter.toVector
      val rs =
        (for ((num, valid) <- runs) yield {
          s"$num${if (valid) "\uD83D\uDC4D\uD83C\uDFFC" else "\uD83D\uDC4E\uD83C\uDFFC"}"
        }).mkString(" ")
      if (runs.length == 1)
        s"$rs"
      else {
        val total = runs.map(_._1.toLong).sum
        s"${runs.length} runs, $total loci (avg %.1f): $rs".format(total.toDouble / runs.length)
      }
    }

    progress(
      s"$validLoci (%.1f%%) valid loci, $invalidLoci invalid ($totalLoci total of ${loci.count} eligible)${overflowMsg}"
        .format(100.0 * validLoci / totalLoci),
      (for {
        (contig, runs) <- depthRunsByContig
      } yield {

        val str =
          GroupRunsIterator[(Int, Boolean)](runs, _._1 < avgRunLength)
            .map(runsStr)
            .mkString("\t\n")

        s"$contig:\t$str"
      }).mkString("\n")
    )

    val lociSets = lociSetsRDD.collect()

    val lociMapBuilder = LociMap.newBuilder[PartitionIndex]()
    for {
      (loci, idx) <- lociSets.zipWithIndex
    } {
      lociMapBuilder.put(loci, idx)
    }
    lociMapBuilder.result()
  }
}
