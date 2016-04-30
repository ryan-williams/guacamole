package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.LociPartitioning
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.readsets.PerSample
import org.hammerlab.guacamole.reference.Region

object ArgsPartitioner extends LociPartitioner[ApproximatePartitionerArgs] {
  override def apply[R <: Region](args: ApproximatePartitionerArgs,
                                  loci: LociSet,
                                  regionRDDs: PerSample[RDD[R]]): LociPartitioning = {
    assume(loci.nonEmpty)

    if (args.partitioningAccuracy == 0) {
      UniformPartitioner(args, loci, regionRDDs)
    } else {
      ApproximatePartitioner(args, loci, regionRDDs)
    }
  }
}
