package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.LociArgs
import org.hammerlab.guacamole.loci.map.LociMap
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.LociPartitioning
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.ReferenceRegion
import org.kohsuke.args4j.{Option => Args4JOption}

import scala.reflect.ClassTag

trait LociPartitionerArgs extends LociArgs

trait AllLociPartitionerArgs
  extends ApproximatePartitionerArgs
    with ExactPartitionerArgs {
  @Args4JOption(name = "--loci-partitioner")
  var lociPartitionerName: String = "exact"
}

trait LociPartitioner {
  def apply[R <: ReferenceRegion: ClassTag](loci: LociSet, regions: RDD[R]): LociPartitioning
}

trait LociPartitionerData {
  def getPartitioner: LociPartitioner
}

object LociPartitioner {
  // Convenience types representing Spark partition indices, or numbers of Spark partitions.
  type PartitionIndex = Int
  type NumPartitions = Int

  type LociPartitioning = LociMap[PartitionIndex]
}
