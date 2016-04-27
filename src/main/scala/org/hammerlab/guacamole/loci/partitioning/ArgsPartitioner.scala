package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.HasReferenceRegion
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.LociPartitioning
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.readsets.PerSample

object ArgsPartitioner extends LociPartitioner[ApproximatePartitionerArgs] {
  override def apply[M <: HasReferenceRegion](args: ApproximatePartitionerArgs,
                                              loci: LociSet,
                                              regionRDDs: PerSample[RDD[M]]): LociPartitioning = {
    assume(loci.nonEmpty)

    if (args.partitioningAccuracy == 0) {
      UniformPartitioner(args, loci, regionRDDs)
    } else {
      ApproximatePartitioner(args, loci, regionRDDs)
    }
  }
}
