package org.hammerlab.guacamole.readsets

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulable, SparkContext}
import org.hammerlab.guacamole.loci.partitioning.{AllLociPartitionerArgs, LociPartitioning}
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.reference.ReferenceRegion
import org.hammerlab.magic.accumulables.{HistogramParam, HashMap => MagicHashMap}
import org.hammerlab.magic.rdd.KeyPartitioner
import org.hammerlab.magic.rdd.SequenceFileSerializableRDD._
import org.hammerlab.magic.util.Stats

import scala.reflect.ClassTag

class PartitionedRegions[R <: ReferenceRegion: ClassTag] private(@transient val regions: RDD[R],
                                                                 @transient val partitioning: LociPartitioning)
  extends Serializable {

  def sc: SparkContext = regions.sparkContext
  def hc: Configuration = sc.hadoopConfiguration

  lazy val partitionLociSets = partitioning.inverse.toArray.sortBy(_._1).map(_._2)
  lazy val lociSetsRDD = sc.parallelize(partitionLociSets, partitionLociSets.length)

  def save(dir: String, overwrite: Boolean = false): Unit = {
    regions.saveCompressed(PartitionedRegions.regionsPath(dir))
    partitioning.save(sc, PartitionedRegions.regionsPath(dir), overwrite)
  }
}

object PartitionedRegions {

  type IntHist = MagicHashMap[Int, Int]

  def IntHist(): IntHist = MagicHashMap[Int, Int]()

  def regionsPath(fn: String) = new Path(fn, "regions")
  def mapPath(fn: String) = new Path(fn, "partitioning")

  def load[R <: ReferenceRegion: ClassTag](sc: SparkContext, fn: String): PartitionedRegions[R] = {
    val regions = sc.fromSequenceFile[R](regionsPath(fn).toString, classOf[BZip2Codec])

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val partitioning = LociPartitioning.load(fs.open(mapPath(fn)))

    new PartitionedRegions(regions, partitioning)
  }

  def apply[R <: ReferenceRegion: ClassTag](regions: RDD[R],
                                            loci: LociSet,
                                            args: AllLociPartitionerArgs,
                                            halfWindowSize: Int = 0): PartitionedRegions[R] = {

    val lociPartitioning = LociPartitioning(regions, loci, args, halfWindowSize)

    progress(
      s"Partitioned loci: ${lociPartitioning.numPartitions} partitions.",
      "Partition-size stats:",
      lociPartitioning.partitionSizeStats.toString(),
      "",
      "Contigs-spanned-per-partition stats:",
      lociPartitioning.partitionContigStats.toString()
    )

    apply(regions, lociPartitioning, halfWindowSize)
  }

  def apply[R <: ReferenceRegion: ClassTag](regions: RDD[R],
                                            partitioning: LociPartitioning): PartitionedRegions[R] =
    apply(regions, partitioning, halfWindowSize = 0)

  def apply[R <: ReferenceRegion: ClassTag](regions: RDD[R],
                                            partitioning: LociPartitioning,
                                            halfWindowSize: Int): PartitionedRegions[R] = {


    val partitioningBroadcast = regions.sparkContext.broadcast(partitioning)

    val numPartitions = partitioning.numPartitions

    implicit val accumulableParam = new HistogramParam[Int, Int]

    val sc = regions.sparkContext

    val regionCopiesHistogram: Accumulable[IntHist, Int] = sc.accumulable(IntHist(), "copies-per-region")

    val partitionRegionsHistogram: Accumulable[IntHist, Int] = sc.accumulable(IntHist(), "regions-per-partition")

    val partitionedRegions =
      (for {
        r <- regions
        partitions = partitioningBroadcast.value.getAll(r, halfWindowSize)
        _ = (regionCopiesHistogram += partitions.size)
        partition <- partitions
      } yield {
        partitionRegionsHistogram += partition
        (partition, r.contig, r.start) -> r
      })
      .repartitionAndSortWithinPartitions(KeyPartitioner(numPartitions))
      .values

    // Need to force materialization for the accumulator to have data… but that's reasonable because anything downstream
    // is presumably going to reuse this RDD.
    // TODO(ryan): worth adding a bypass here / pushing the printing of these statistics to later, for applications that
    // want to save this ~extra materialization. Worth benchmarking, in any case.
    val totalReadCopies = partitionedRegions.count

    val totalReads = regions.count

    val regionCopies = regionCopiesHistogram.value.toArray.sortBy(_._1)

    val readsPlaced = regionCopies.map(_._2.toLong).sum

    val regionsPerPartition = partitionRegionsHistogram.value.toArray.sortBy(_._1)

    progress(
      s"Partitioned reads, placed $readsPlaced of $totalReads (%.1f%%)) %.1fx on avg; copies per read histogram:"
        .format(
          100.0 * totalReadCopies / totalReads,
          totalReadCopies * 1.0 / readsPlaced
        ),
      (for {
        (numCopies, numReads) <- regionCopies
      } yield
        s"$numCopies:\t$numReads"
      ).mkString("\n"),
      "",
      "Reads per partition stats:",
      Stats(regionsPerPartition.map(_._2)).toString()
    )

    new PartitionedRegions(partitionedRegions, partitioning)
  }

//  implicit def unwrapPartitionedRegions[R <: ReferenceRegion: ClassTag](pr: PartitionedRegions[R]): RDD[R] = pr.regions
}
