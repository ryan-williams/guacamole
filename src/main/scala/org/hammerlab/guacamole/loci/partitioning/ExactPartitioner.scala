package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.map.LociMap
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.PartitionIndex
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.readsets.RegionRDD
import org.hammerlab.guacamole.readsets.RegionRDD._
import org.hammerlab.guacamole.reference.ReferenceRegion
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
    val depthRuns = depthRunsRDD.take(numDepthRunsToTake)

    val overflowMsg =
      if (numDepthRuns > numDepthRunsToTake)
        s". First $numDepthRunsToTake runs:"
      else
        ":"

    progress(
      s"$validLoci (%.1f%%) valid loci, $invalidLoci invalid ($totalLoci total of ${loci.count} eligible)${overflowMsg}"
        .format(100.0 * validLoci / totalLoci),
      (for {
        ((contig, validDepth), numLoci) <- depthRuns
      } yield
        s"$contig:$validDepth\t$numLoci"
      ).mkString("\n")
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
