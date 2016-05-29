package org.hammerlab.guacamole.readsets

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.zip.ZipPartitionsWithIndexRDD._
import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.loci.Coverage.PositionCoverage
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.{ReferencePosition, ReferenceRegion}
import org.hammerlab.magic.rdd.BorrowElemsRDD._
import org.hammerlab.magic.rdd.PartitionFirstElemsRDD._
import org.hammerlab.magic.rdd.RDDStats._
import org.hammerlab.magic.util.Stats

import org.hammerlab.magic.rdd.RunLengthRDD._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class RegionRDD[R <: ReferenceRegion: ClassTag](@transient rdd: RDD[R],
                                                contigLengthsBroadcast: Broadcast[ContigLengths])
  extends Serializable {

  @transient lazy val (
    numEmptyPartitions: Int,
    numPartitionsSpanningContigs: Int,
    partitionSpans: ArrayBuffer[Long],
    spanStats: Stats,
    nonEmptySpanStats: Stats
  ) = {
    val partitionBounds = rdd.partitionBounds
    var numEmpty = 0
    var numCrossingContigs = 0
    val spans = ArrayBuffer[Long]()
    val nonEmptySpans = ArrayBuffer[Long]()

    partitionBounds.foreach {
      case None =>
        numEmpty += 1
        spans += 0L
      case Some((first, last)) if first.contig == last.contig =>
        val span = last.start - first.start
        nonEmptySpans += span
        spans += span
      case _ =>
        numCrossingContigs += 1
        None
    }

    (numEmpty, numCrossingContigs, spans, Stats(spans), Stats(nonEmptySpans))
  }

  def shuffleCoverage(halfWindowSize: Int): RDD[PositionCoverage] = {
    rdd
      .flatMap(r => {
        val c = r.contig
        val length = contigLengthsBroadcast.value(c)

        val lowerBound = math.max(0, r.start - halfWindowSize)
        val upperBound = math.min(length, r.end + halfWindowSize)

        val outs = ArrayBuffer[(ReferencePosition, Coverage)]()
        for {
          l <- lowerBound until upperBound
        } {
          outs += ReferencePosition(c, l) -> Coverage(depth = 1)
        }

        outs += ReferencePosition(c, lowerBound) -> Coverage(starts = 1)
        outs += ReferencePosition(c, upperBound) -> Coverage(ends = 1)

        outs.iterator
      })
      .reduceByKey(_ + _)
      .sortByKey()
  }

  @transient val coverages_ = mutable.Map[Int, RDD[PositionCoverage]]()
  def coverage(halfWindowSize: Int): RDD[PositionCoverage] =
    coverages_.getOrElseUpdate(
      halfWindowSize,
      {
        rdd
          .mapPartitions(it =>
            for {
              contigIterator <- new ContigsIterator(it.buffered)
              length = contigLengthsBroadcast.value(contigIterator.contig)
              coverage <- CoverageIterator(halfWindowSize, length, contigIterator)
            } yield
              coverage
          )
          .reduceByKey(_ + _)
          .sortByKey()
      }
    )

  @transient val depths_ = mutable.Map[Int, RDD[(Int, ReferencePosition)]]()
  def depths(halfWindowSize: Int): RDD[(Int, ReferencePosition)] =
    depths_.getOrElseUpdate(
      halfWindowSize,
      coverage(halfWindowSize).map(t => t._2.depth -> t._1).sortByKey(ascending = false)
    )

  @transient val starts_ = mutable.Map[Int, RDD[(Int, ReferencePosition)]]()
  def starts(halfWindowSize: Int): RDD[(Int, ReferencePosition)] =
    starts_.getOrElseUpdate(
      halfWindowSize,
      coverage(halfWindowSize).map(t => t._2.starts -> t._1).sortByKey(ascending = false)
    )

  @transient val ends_ = mutable.Map[Int, RDD[(Int, ReferencePosition)]]()
  def ends(halfWindowSize: Int): RDD[(Int, ReferencePosition)] =
    ends_.getOrElseUpdate(
      halfWindowSize,
      coverage(halfWindowSize).map(t => t._2.ends -> t._1).sortByKey(ascending = false)
    )

  def slidingLociWindow(halfWindowSize: Int, loci: LociSet): RDD[(ReferencePosition, Iterable[R])] = {
    val copiedRegionsRDD: RDD[R] = rdd.copyFirstElems(BoundedContigIterator(halfWindowSize + 1, _))

    val boundsRDD = rdd.map(_.endPos + halfWindowSize).elemBoundsRDD

    copiedRegionsRDD.zipPartitionsWithIndex(boundsRDD)(
        (idx, readsIter, boundsIter) => {
          val (fromOpt, untilOpt) = boundsIter.next()
          val bufferedReads = readsIter.buffered

          new WindowIterator(
            halfWindowSize,
            if (idx > 0)
              fromOpt
            else
              None,
            untilOpt,
            loci,
            bufferedReads
          )
        }
      )
  }

  def partitionDepths(halfWindowSize: Int, depthCutoff: Int): RDD[((String, Boolean), Int)] = {
    coverage(halfWindowSize).map(t => t._1.contig -> (t._2.depth >= depthCutoff)).runLengthEncode
  }

//  def partition(halfWindowSize: Int,
//                loci: LociSet,
//                regionsPerPartition: Int,
//                maxRegionsPerPartition: Int): RDD[(ReferencePosition, Iterable[R])] = {
//
//    implicit val orderByStart = ReferenceRegion.orderByStart[R]
//    val sorted = rdd.sort()



    //val partitioner = sorted.partitioner.get.asInstanceOf[RangePartitioner]
    //partitioner.rangeBounds

//    rdd.mapPartitionsWithIndex((idx, it) => {
//
//    })
//  }
}

object RegionRDD {
  private val rddMap = mutable.Map[Int, RegionRDD[_]]()
  implicit def rddToRegionRDD[R <: ReferenceRegion: ClassTag](
    rdd: RDD[R]
  )(
    implicit ordering: PartialOrdering[R],
    contigLengthsBroadcast: Broadcast[ContigLengths]
  ): RegionRDD[R] =
    rddMap.getOrElseUpdate(
      rdd.id,
      new RegionRDD[R](rdd, contigLengthsBroadcast)
    ).asInstanceOf[RegionRDD[R]]
}
