package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.Accumulable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.PartitionIndex
import org.hammerlab.guacamole.loci.partitioning.LociPartitioning
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.reference.ReferenceRegion
import org.hammerlab.magic.accumulables.{HistogramParam, HashMap => MagicHashMap}
import org.hammerlab.magic.rdd.KeyPartitioner
import org.hammerlab.magic.util.Stats

import scala.reflect.ClassTag

object PartitionedRegions {

  type IntHist = MagicHashMap[Int, Long]

  def IntHist(): IntHist = MagicHashMap[Int, Long]()

  def apply[R <: ReferenceRegion: ClassTag](regions: RDD[R],
                                            partitioningBroadcast: Broadcast[LociPartitioning],
                                            halfWindowSize: Int,
                                            printStats: Boolean = true): RDD[R] = {

    val sc = regions.sparkContext

    val lociPartitioning = partitioningBroadcast.value

    val numPartitions = lociPartitioning.numPartitions

    progress(s"Partitioning reads according to loci partitioning:\n$lociPartitioning")

    val numTasks = lociPartitioning.inverse.map(_._1).max + 1

    implicit val accumulableParam = new HistogramParam[Int, Long]

    // Histogram of the number of copies made of each region (i.e. when a region straddles loci-partition
    // boundaries.
    val regionCopiesHistogram: Accumulable[IntHist, Int] = sc.accumulable(IntHist(), "copies-per-region")

    // Histogram of the number of regions assigned to each partition.
    val partitionRegionsHistogram: Accumulable[IntHist, Int] = sc.accumulable(IntHist(), "regions-per-partition")

    val partitionedRegions =
      (for {
        r <- regions
        partitions = partitioningBroadcast.value.getAll(r, halfWindowSize)
        _ = (regionCopiesHistogram += partitions.size)
        partition <- partitions
      } yield {
        partitionRegionsHistogram += partition
        (partition, r.contigName, r.start) -> r
      })
      .repartitionAndSortWithinPartitions(KeyPartitioner(numPartitions))
      .values
      .setName("partitioned-regions")

    if (printStats) {
      // Need to force materialization for the accumulator to have data… but that's reasonable because anything
      // downstream is presumably going to reuse this RDD.
      val totalReadCopies = partitionedRegions.count

      val originalReads = regions.count

      // Sorted array of [number of read copies "K"] -> [number of reads that were copied "K" times].
      val regionCopies: Array[(Int, Long)] = regionCopiesHistogram.value.toArray.sortBy(_._1)

      // Number of distinct reads that were sent to at least one partition.
      val readsPlaced = regionCopies.filter(_._1 > 0).map(_._2).sum

      // Sorted array: [partition index "K"] -> [number of reads assigned to partition "K"].
      val regionsPerPartition: Array[(PartitionIndex, Long)] = partitionRegionsHistogram.value.toArray.sortBy(_._1)

      progress(
        s"Placed $readsPlaced of $originalReads (%.1f%%), %.1fx copies on avg; copies per read histogram:"
        .format(
          100.0 * readsPlaced / originalReads,
          totalReadCopies * 1.0 / readsPlaced
        ),
        Stats.fromHist(regionCopies).toString(),
        "",
        "Reads per partition stats:",
        Stats(regionsPerPartition.map(_._2)).toString()
      )

    }

    partitionedRegions
  }
}
