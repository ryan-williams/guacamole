package org.hammerlab.guacamole.readsets

//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.rdd.RDD
//import org.hammerlab.guacamole.loci.map.LociMap
//import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.{LociPartitioning, PartitionIndex}
//import org.hammerlab.guacamole.loci.set.{LociSet, TakeLociIterator}
//import org.hammerlab.guacamole.reference.{ReferencePosition, ReferenceRegion}
//import org.hammerlab.magic.rdd.KeyPartitioner
//import org.hammerlab.magic.rdd.RunLengthRDD._
//import org.hammerlab.guacamole.loci.Coverage
//import org.hammerlab.guacamole.loci.Coverage.PositionCoverage
//
//import scala.collection.mutable
//import scala.collection.mutable.ArrayBuffer
//import scala.reflect.ClassTag
//
//class PartitionRegionsRDD[R <: ReferenceRegion: ClassTag](@transient rdd: RDD[R],
//                                                          implicit val contigLengthsBroadcast: Broadcast[ContigLengths])
//  extends Serializable {
//
//  @transient val sc = rdd.sparkContext
//
//  def shuffleCoverage(halfWindowSize: Int): RDD[PositionCoverage] = {
//    rdd
//      .flatMap(r => {
//        val c = r.contig
//        val length = contigLengthsBroadcast.value(c)
//
//        val lowerBound = math.max(0, r.start - halfWindowSize)
//        val upperBound = math.min(length, r.end + halfWindowSize)
//
//        val outs = ArrayBuffer[(ReferencePosition, Coverage)]()
//        for {
//          l <- lowerBound until upperBound
//        } {
//          outs += ReferencePosition(c, l) -> Coverage(depth = 1)
//        }
//
//        outs += ReferencePosition(c, lowerBound) -> Coverage(starts = 1)
//        outs += ReferencePosition(c, upperBound) -> Coverage(ends = 1)
//
//        outs.iterator
//      })
//      .reduceByKey(_ + _)
//      .sortByKey()
//  }
//
//  @transient lazy val allLoci = LociSet.all(contigLengthsBroadcast.value)
//  @transient lazy val allLociBroadcast = sc.broadcast(allLoci)
//
//  @transient val coverages_ = mutable.Map[(Int, LociSet), RDD[PositionCoverage]]()
//  def coverage(halfWindowSize: Int, lociBroadcast: Broadcast[LociSet] = allLociBroadcast): RDD[PositionCoverage] =
//    coverages_.getOrElseUpdate(
//      (halfWindowSize, lociBroadcast.value),
//      {
//        rdd
//        .mapPartitions(it =>
//          for {
//            (contigRegionsIterator, contigLociIterator) <- new ContigsIterator(it.buffered, lociBroadcast.value)
//            length = contigLengthsBroadcast.value(contigRegionsIterator.contig)
//            coverage <- ContigCoverageIterator(halfWindowSize, length, contigRegionsIterator, contigLociIterator)
//          } yield
//            coverage
//        )
//        .reduceByKey(_ + _)
//        .sortByKey()
//      }
//    )
//
//  @transient val depths_ = mutable.Map[Int, RDD[(Int, ReferencePosition)]]()
//  def depths(halfWindowSize: Int): RDD[(Int, ReferencePosition)] =
//    depths_.getOrElseUpdate(
//      halfWindowSize,
//      coverage(halfWindowSize).map(t => t._2.depth -> t._1).sortByKey(ascending = false)
//    )
//
//  @transient val starts_ = mutable.Map[Int, RDD[(Int, ReferencePosition)]]()
//  def starts(halfWindowSize: Int): RDD[(Int, ReferencePosition)] =
//    starts_.getOrElseUpdate(
//      halfWindowSize,
//      coverage(halfWindowSize).map(t => t._2.starts -> t._1).sortByKey(ascending = false)
//    )
//
//  @transient val ends_ = mutable.Map[Int, RDD[(Int, ReferencePosition)]]()
//  def ends(halfWindowSize: Int): RDD[(Int, ReferencePosition)] =
//    ends_.getOrElseUpdate(
//      halfWindowSize,
//      coverage(halfWindowSize).map(t => t._2.ends -> t._1).sortByKey(ascending = false)
//    )
//
//  def partitionDepths(halfWindowSize: Int, depthCutoff: Int): RDD[((String, Boolean), Int)] = {
//    coverage(halfWindowSize).map(t => t._1.contig -> (t._2.depth >= depthCutoff)).runLengthEncode
//  }
//
//  def makeCappedLociSets(halfWindowSize: Int,
//                         maxRegionsPerPartition: Int): RDD[LociSet] =
//    coverage(halfWindowSize).mapPartitionsWithIndex((idx, it) =>
//      new TakeLociIterator(it.buffered, maxRegionsPerPartition)
//    )
//
//  def getPartitioning(halfWindowSize: Int,
//                      maxRegionsPerPartition: Int): LociPartitioning = {
//    val lociSetsRDD = makeCappedLociSets(halfWindowSize, maxRegionsPerPartition)
//    val lociSets = lociSetsRDD.collect()
//
//    val lociMapBuilder = LociMap.newBuilder[PartitionIndex]()
//    for {
//      (loci, idx) <- lociSets.zipWithIndex
//    } {
//      lociMapBuilder.put(loci, idx)
//    }
//    lociMapBuilder.result()
//  }
//
//  def partition(halfWindowSize: Int,
//                maxRegionsPerPartition: Int): RDD[R] =
//    partition(halfWindowSize, getPartitioning(halfWindowSize, maxRegionsPerPartition))
//
//  def partition(halfWindowSize: Int,
//                partitioning: LociPartitioning): RDD[R] = {
//    val partitioningBroadcast = sc.broadcast(partitioning)
//
//    val numPartitions = partitioning.inverse.size
//
//    (for {
//      r <- rdd
//      partition <- partitioningBroadcast.value.getAll(r, halfWindowSize)
//    } yield
//      (partition, r.contig, r.start) -> r
//      )
//    .repartitionAndSortWithinPartitions(KeyPartitioner(numPartitions))
//    .values
//  }
//}
//
//object PartitionRegionsRDD {
//  implicit def rddToPartitionRegionsRDD[R <: ReferenceRegion: ClassTag](
//    rdd: RDD[R]
//  )(
//    implicit contigLengthsBroadcast: Broadcast[ContigLengths]
//  ): PartitionRegionsRDD[R] =
//    new PartitionRegionsRDD[R](rdd, contigLengthsBroadcast)
//}
