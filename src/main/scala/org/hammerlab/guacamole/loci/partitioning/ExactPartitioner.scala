package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.map.LociMap
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.{LociPartitioning, PartitionIndex}
import org.hammerlab.guacamole.loci.set.LociSet
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

  @Args4jOption(
    name = "--partitioned-reads-path",
    usage = "Directory from which to read an existing partition-reads RDD, with accompanying LociMap partitioning."
  )
  var partitionedReadsPath: String = ""

  @Args4jOption(
    name = "--save-partitioning",
    usage = "Directory path within which to save the partitioned reads and accompanying LociMap partitioning."
  )
  var savePartitioningPath: String = ""
}

class ExactPartitioner[R <: ReferenceRegion: ClassTag](regions: RDD[R],
                                                       halfWindowSize: Int,
                                                       maxRegionsPerPartition: Int)
  extends LociPartitioner {

  def partition(loci: LociSet): LociPartitioning = {
    val lociSetsRDD = regions.makeCappedLociSets(loci, halfWindowSize, maxRegionsPerPartition)
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
