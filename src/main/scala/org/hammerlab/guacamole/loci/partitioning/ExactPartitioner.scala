package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.LociPartitioning
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.readsets.ContigLengths
import org.hammerlab.guacamole.readsets.RegionRDD._
import org.hammerlab.guacamole.reference.ReferenceRegion
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.reflect.ClassTag

trait ExactPartitionerArgs extends LociPartitionerArgs {
  @Args4jOption(
    name = "--max-reads-per-partition",
    usage = "Maximum number of reads to allow any one partition to have. Loci that have more depth than this will be dropped."
  )
  var maxReadsPerPartition: Int = 500000/*,
  @Args4jOption(
    name = "--half-window-size",
    usage = "Maximum number of reads to allow any one partition to have. Loci that have more depth than this will be dropped."
  )
  maxReadsPerPartition: Int = 500000*/
}

class ExactPartitioner(halfWindowSize: Int,
                       maxRegionsPerPartition: Int,
                       contigLengthsBroadcast: Broadcast[ContigLengths])
  extends LociPartitioner {

  override def apply[R <: ReferenceRegion : ClassTag](loci: LociSet, regions: RDD[R]): LociPartitioning = {
    implicit val clb = contigLengthsBroadcast
    regions.getPartitioning(halfWindowSize, maxRegionsPerPartition)
  }

}

//object ExactPartitioner {
//  def apply(halfWindowSize: Int,
//            maxRegionsPerPartition: Int,
//            contigLengthsBroadcast: Broadcast[ContigLengths],
//            loci: LociSet)
//}
