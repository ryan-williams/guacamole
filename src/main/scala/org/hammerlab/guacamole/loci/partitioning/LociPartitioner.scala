package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.LociArgs
import org.hammerlab.guacamole.loci.map.LociMap
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.LociPartitioning
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.{HasReferenceRegion, PerSample}

trait LociPartitionerArgs extends LociArgs

trait LociPartitioner[Args <: LociPartitionerArgs] {
  def apply[M <: HasReferenceRegion](args: Args,
                                         loci: LociSet,
                                         regionsRDD: RDD[M]): LociPartitioning =
    apply(args, loci, Vector(regionsRDD))

  def apply[M <: HasReferenceRegion](args: Args, loci: LociSet, regionsRDDs: PerSample[RDD[M]]): LociPartitioning
}

object LociPartitioner {
  // Convenience types representing Spark partition indices, or numbers of Spark partitions.
  type PartitionIdx = Int
  type NumPartitions = Int

  type LociPartitioning = LociMap[PartitionIdx]
}
