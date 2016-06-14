package org.hammerlab.guacamole.readsets

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.map.LociMap
import org.hammerlab.guacamole.loci.partitioning.AllLociPartitionerArgs
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.{LociPartitioning, PartitionIndex}
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.reference.ReferenceRegion
import org.hammerlab.magic.rdd.KeyPartitioner
import org.hammerlab.magic.rdd.SequenceFileSerializableRDD._

import scala.reflect.ClassTag

class PartitionedRegions[R <: ReferenceRegion: ClassTag] private(@transient val regions: RDD[R],
                                                                 @transient val partitioning: LociMap[PartitionIndex])
  extends Serializable {

  def sc: SparkContext = regions.sparkContext
  def hc: Configuration = sc.hadoopConfiguration

  lazy val partitionLociSets = partitioning.inverse.toArray.sortBy(_._1).map(_._2)
  lazy val lociSetsRDD = sc.parallelize(partitionLociSets, partitionLociSets.length)

  def save(fn: String): Unit = {
    val fs = FileSystem.get(hc)

    regions.saveCompressed(PartitionedRegions.regionsPath(fn))
    val mapOut = fs.create(PartitionedRegions.mapPath(fn))

    partitioning.prettyPrint(mapOut)
    mapOut.close()
  }
}

object PartitionedRegions {

  type PartitionedReads = PartitionedRegions[MappedRead]

  def regionsPath(fn: String) = new Path(fn, "regions")
  def mapPath(fn: String) = new Path(fn, "partitioning")

  def load[R <: ReferenceRegion: ClassTag](sc: SparkContext, fn: String): PartitionedRegions[R] = {
    val regions = sc.fromSequenceFile[R](regionsPath(fn).toString, classOf[BZip2Codec])

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val partitioning = LociMap.load(fs.open(mapPath(fn)))

    new PartitionedRegions(regions, partitioning)
  }

  def apply[R <: ReferenceRegion: ClassTag](regions: RDD[R],
                                            loci: LociSet,
                                            args: AllLociPartitionerArgs,
                                            halfWindowSize: Int = 0): PartitionedRegions[R] =
    apply(
      regions,
      args
        .getPartitioner(regions, halfWindowSize)
        .partition(loci),
      halfWindowSize
    )

  def apply[R <: ReferenceRegion: ClassTag](regions: RDD[R],
                                            partitioning: LociPartitioning): PartitionedRegions[R] =
    apply(regions, partitioning, halfWindowSize = 0)

  def apply[R <: ReferenceRegion: ClassTag](regions: RDD[R],
                                            partitioning: LociPartitioning,
                                            halfWindowSize: Int): PartitionedRegions[R] = {

    val partitioningBroadcast = regions.sparkContext.broadcast(partitioning)

    val numPartitions = partitioning.inverse.size

    val partitionedRegions =
      (for {
        r <- regions
        partition <- partitioningBroadcast.value.getAll(r, halfWindowSize)
      } yield
        (partition, r.contig, r.start) -> r
      )
      .repartitionAndSortWithinPartitions(KeyPartitioner(numPartitions))
      .values

    new PartitionedRegions(partitionedRegions, partitioning)
  }

  implicit def partitionedReadsRDDToRDDMappedRead[R <: ReferenceRegion: ClassTag](prRDD: PartitionedRegions[R]): RDD[R] = prRDD.regions
}
