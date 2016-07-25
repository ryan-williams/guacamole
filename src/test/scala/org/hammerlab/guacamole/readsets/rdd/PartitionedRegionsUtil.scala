package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.partitioning.LociPartitioning
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.readsets.{ContigLengths, PartitionedReads, PerSample, ReadSets}
import org.hammerlab.guacamole.reference.ReferenceRegion

import scala.reflect.ClassTag

trait PartitionedRegionsUtil {

  def partitionReads[R <: ReferenceRegion: ClassTag](readsRDDs: PerSample[RDD[R]],
                                                     lociPartitioning: LociPartitioning): PartitionedRegions[R] = {
    PartitionedRegions(
      readsRDDs,
      lociPartitioning,
      halfWindowSize = 0,
      partitionedRegionsPathOpt = None,
      compress = false,
      printStats = false
    )
  }

  def makePartitionedReads(readsets: ReadSets,
                           halfWindowSize: Int,
                           maxRegionsPerPartition: Int,
                           numPartitions: Int)(implicit contigLengths: ContigLengths): PartitionedReads = {
    val args = new PartitionedRegionsArgs {}
    args.parallelism = numPartitions
    args.maxReadsPerPartition = maxRegionsPerPartition

    PartitionedRegions(readsets.mappedReadsRDDs, LociSet.all(contigLengths), args, halfWindowSize)
  }
}
