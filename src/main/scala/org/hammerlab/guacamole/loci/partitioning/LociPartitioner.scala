package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.LociArgs
import org.hammerlab.guacamole.loci.map.LociMap
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.LociPartitioning
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.readsets.PerSample
import org.hammerlab.guacamole.reference.ReferenceRegion

import scala.reflect.ClassTag

trait LociPartitionerArgs extends LociArgs

trait LociPartitioner[Args <: LociPartitionerArgs] {
  def apply[R <: ReferenceRegion: ClassTag](args: Args,
                                            loci: LociSet,
                                            regionsRDD: RDD[R]): LociPartitioning =
    apply(args, loci, Vector(regionsRDD))

  def apply[R <: ReferenceRegion: ClassTag](args: Args, loci: LociSet, regionsRDDs: PerSample[RDD[R]]): LociPartitioning
}

object LociPartitioner {
  // Convenience types representing Spark partition indices, or numbers of Spark partitions.
  type PartitionIndex = Int
  type NumPartitions = Int

  type LociPartitioning = LociMap[PartitionIndex]
}
