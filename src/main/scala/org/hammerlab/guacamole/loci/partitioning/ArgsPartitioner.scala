package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.LociPartitioning
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.ReferenceRegion

import scala.reflect.ClassTag

class ArgsPartitioner(args: ApproximatePartitionerArgs) extends LociPartitioner {
  override def apply[R <: ReferenceRegion: ClassTag](loci: LociSet, rdd: RDD[R]): LociPartitioning = {
    assume(loci.nonEmpty)

    if (args.partitioningAccuracy == 0)
      new UniformPartitioner(args.parallelism).partition(loci)
    else
      new ApproximatePartitioner(args).apply(loci, rdd)
  }
}
