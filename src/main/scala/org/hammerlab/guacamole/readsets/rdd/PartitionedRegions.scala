package org.hammerlab.guacamole.readsets.rdd

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulable, SparkContext}
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.PartitionIndex
import org.hammerlab.guacamole.loci.partitioning.{LociPartitionerArgs, LociPartitioning}
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.reference.ReferenceRegion
import org.hammerlab.magic.accumulables.{HistogramParam, HashMap => MagicHashMap}
import org.hammerlab.magic.rdd.KeyPartitioner
import org.hammerlab.magic.util.Stats

import scala.reflect.ClassTag

/**
 * Groups a {{LociPartitioning}} with an RDD[ReferenceRegion] that has already been partitioned according to the
 * partitioning.
 *
 * This means some regions will occur multiple times in the RDD (due to regions straddling partition boundaries, so it's
 * important not to confuse this with a regular RDD[ReferenceRegion].
 */
class PartitionedRegions[R <: ReferenceRegion: ClassTag](@transient val regions: RDD[R],
                                                         @transient val partitioning: LociPartitioning)
  extends Serializable {

  def sc: SparkContext = regions.sparkContext
  def hc: Configuration = sc.hadoopConfiguration

  lazy val partitionLociSets: Array[LociSet] =
    partitioning
      .inverse
      .toArray
      .sortBy(_._1)
      .map(_._2)

  lazy val lociSetsRDD: RDD[LociSet] =
    sc
      .parallelize(partitionLociSets, partitionLociSets.length)
      .setName("lociSetsRDD")
}

object PartitionedRegions {

  type IntHist = MagicHashMap[Int, Long]

  def IntHist(): IntHist = MagicHashMap[Int, Long]()

  def apply[R <: ReferenceRegion: ClassTag](regions: RDD[R],
                                            loci: LociSet,
                                            args: LociPartitionerArgs): PartitionedRegions[R] =
    apply(regions, loci, args, halfWindowSize = 0)

  def apply[R <: ReferenceRegion: ClassTag](regions: RDD[R],
                                            loci: LociSet,
                                            args: LociPartitionerArgs,
                                            halfWindowSize: Int): PartitionedRegions[R] = {

    val lociPartitioning = LociPartitioning(regions, loci, args, halfWindowSize)

    progress(
      s"Partitioned loci: ${lociPartitioning.numPartitions} partitions.",
      "Partition-size stats:",
      lociPartitioning.partitionSizeStats.toString(),
      "",
      "Contigs-spanned-per-partition stats:",
      lociPartitioning.partitionContigStats.toString()
    )

    apply(
      regions,
      lociPartitioning,
      halfWindowSize,
      !args.quiet
    )
  }

  def apply[R <: ReferenceRegion: ClassTag](regions: RDD[R],
                                            lociPartitioning: LociPartitioning,
                                            halfWindowSize: Int,
                                            printStats: Boolean): PartitionedRegions[R] = {

    val sc = regions.sparkContext

    val partitioningBroadcast = regions.sparkContext.broadcast(lociPartitioning)

    val numPartitions = lociPartitioning.numPartitions

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

    new PartitionedRegions(partitionedRegions, lociPartitioning)
  }
}
