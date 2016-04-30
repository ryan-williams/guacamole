package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.map.LociMap
import org.hammerlab.guacamole.loci.partitioning.ApproximatePartitioner.MicroPartitionIdx
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.{LociPartitioning, NumPartitions, PartitionIdx}
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.logging.DebugLogArgs
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.readsets.PerSample
import org.hammerlab.guacamole.reference.Region
import org.kohsuke.args4j.{Option => Args4jOption}
import spire.implicits._
import spire.math.Integral

trait UniformPartitionerArgs
  extends DebugLogArgs
    with LociPartitionerArgs {

  @Args4jOption(
    name = "--parallelism",
    usage = "Number of variant calling partitions. Set to 0 (default) to use the number of Spark partitions."
  )
  var parallelism: NumPartitions = 0
}

object UniformPartitioner extends LociPartitioner[UniformPartitionerArgs] {

  override def apply[R <: Region](args: UniformPartitionerArgs,
                                  loci: LociSet,
                                  unused: PerSample[RDD[R]]): LociPartitioning =
    partition(args.parallelism, loci)

  def apply[R <: Region](numPartitions: PartitionIdx, loci: LociSet): LociMap[PartitionIdx] =
    partition(numPartitions, loci)

  def apply[R <: Region](numMicroPartitions: MicroPartitionIdx, loci: LociSet): LociMap[MicroPartitionIdx] =
    partition(numMicroPartitions, loci)

  /**
   * Assign loci to partitions. Contiguous intervals of loci will tend to get assigned to the same partition.
   *
   * This implementation assigns loci uniformly, i.e. each partition gets about the same number of loci. A smarter
   * implementation, partitionLociByApproximateDepth, can be found below; it considers the depth of coverage at each
   * locus and assigns each partition loci corresponding to approximately the same number of regions.
   *
   * @param numPartitions Number of partitions; these needn't map directly to a number of Spark partitions, since e.g.
   *                      partitionLociByApproximateDepth does post-processing on a large number of "micro-partitions"
   *                      computed here, combining them to generate partitions that are passed to Spark.
   * @param loci Loci to partition
   * @return LociMap of locus -> partition assignments
   */
  private def partition[N: Integral](numPartitions: N, loci: LociSet): LociMap[N] = {

    assume(numPartitions >= 1, "`numPartitions` (--parallelism) should be >= 1")

    val lociPerPartition = math.max(1, loci.count.toDouble / numPartitions.toDouble())

    progress(
      "Splitting loci evenly among %,d numPartitions = ~%,.0f loci per partition"
      .format(numPartitions.toLong(), lociPerPartition)
    )

    var lociAssigned = 0L

    var partition = Integral[N].zero

    def remainingForThisPartition = math.round(((partition + 1).toDouble * lociPerPartition) - lociAssigned)

    val builder = LociMap.newBuilder[N]

    for {
      contig <- loci.contigs
      range <- contig.ranges
    } {
      var start = range.start
      val end = range.end
      while (start < end) {
        val length: Long = math.min(remainingForThisPartition, end - start)
        builder.put(contig.name, start, start + length, partition)
        start += length
        lociAssigned += length
        if (remainingForThisPartition == 0) partition += 1
      }
    }

    val result = builder.result
    assert(lociAssigned == loci.count)
    assert(result.count == loci.count)
    result
  }
}
