package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.LociArgs
import org.hammerlab.guacamole.loci.map.LociMap
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.LociPartitioning
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.readsets.PerSample
import org.hammerlab.guacamole.reference.ReferenceRegion

trait LociPartitionerArgs extends LociArgs

trait LociPartitioner[Args <: LociPartitionerArgs] {
  def apply[M <: ReferenceRegion](args: Args,
                                         loci: LociSet,
                                         regionsRDD: RDD[M]): LociPartitioning =
    apply(args, loci, Vector(regionsRDD))

  def apply[M <: ReferenceRegion](args: Args, loci: LociSet, regionsRDDs: PerSample[RDD[M]]): LociPartitioning
}

object LociPartitioner {
  // Convenience types representing Spark partition indices, or numbers of Spark partitions.
  type PartitionIndex = Int
  type NumPartitions = Int

  type LociPartitioning = LociMap[PartitionIndex]
}
