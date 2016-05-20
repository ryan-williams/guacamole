package org.hammerlab.guacamole.readsets

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.loci.Coverage.PositionCoverage
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.rdd.PartitionFirstElemsRDD._
import org.hammerlab.guacamole.rdd.RDDStats._
import org.hammerlab.guacamole.reference.{ReferencePosition, ReferenceRegion}
import org.hammerlab.guacamole.util.Stats

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class RegionRDD[R <: ReferenceRegion: ClassTag] private(rdd: RDD[R]) {

  lazy val (
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

  def sc: SparkContext = rdd.sparkContext

  def shuffleCoverage(contigLengths: ContigLengths, halfWindowSize: Int): RDD[PositionCoverage] = {
    val contigLengthsBroadcast = sc.broadcast(contigLengths)
    rdd.flatMap(r => {
      val c = r.contig
      val length = contigLengthsBroadcast.value(c)

      val lowerBound = r.start - halfWindowSize
      val upperBound = r.end + halfWindowSize

      val outs = ArrayBuffer[(ReferencePosition, Coverage)]()
      for {
        l <- math.max(0, lowerBound) until math.min(length, upperBound)
      } {
        outs += ReferencePosition(c, l) -> Coverage(depth = 1)
      }

      outs += ReferencePosition(c, lowerBound) -> Coverage(starts = 1)
      outs += ReferencePosition(c, upperBound) -> Coverage(ends = 1)

      outs.iterator
    }).reduceByKey(_ + _).sortByKey()
  }

  val coverages_ = mutable.Map[Int, RDD[PositionCoverage]]()
  def coverage(halfWindowSize: Int): RDD[PositionCoverage] =
    coverages_.getOrElseUpdate(
      halfWindowSize,
      rdd
        .mapPartitions(it =>
          for {
            contigIterator <- ContigsIterator(it)
            coverage <- CoverageIterator(halfWindowSize, contigIterator)
          } yield
            coverage
        )
        .reduceByKey(_ + _)
        .sortByKey()
    )

  val depths_ = mutable.Map[Int, RDD[(Int, ReferencePosition)]]()
  def depths(halfWindowSize: Int): RDD[(Int, ReferencePosition)] =
    depths_.getOrElseUpdate(
      halfWindowSize,
      coverage(halfWindowSize).map(t => t._2.depth -> t._1).sortByKey(ascending = false)
    )

  val starts_ = mutable.Map[Int, RDD[(Int, ReferencePosition)]]()
  def starts(halfWindowSize: Int): RDD[(Int, ReferencePosition)] =
    starts_.getOrElseUpdate(
      halfWindowSize,
      coverage(halfWindowSize).map(t => t._2.starts -> t._1).sortByKey(ascending = false)
    )

  val ends_ = mutable.Map[Int, RDD[(Int, ReferencePosition)]]()
  def ends(halfWindowSize: Int): RDD[(Int, ReferencePosition)] =
    ends_.getOrElseUpdate(
      halfWindowSize,
      coverage(halfWindowSize).map(t => t._2.ends -> t._1).sortByKey(ascending = false)
    )

  def slidingLociWindow(halfWindowSize: Int, loci: LociSet): RDD[(ReferencePosition, (Iterable[R], Int, Int))] = {
    val firstRegionsRDD: RDD[R] = rdd.borrowFirstElems(BoundedContigIterator(2 * halfWindowSize + 1, _))

    val boundsRDD = rdd.firstElemBoundsRDD(_.startPos + halfWindowSize + 1)

    implicit def toPos[T](t: (ReferencePosition, T)): ReferencePosition = t._1

    rdd
      .zipPartitions(firstRegionsRDD, boundsRDD)(
        (readsIter, lastReadsIter, boundsIter) => {
          val (fromOpt, untilOpt) = boundsIter.next()
          val bufferedReads = (readsIter ++ lastReadsIter).buffered

          new WindowIterator(halfWindowSize, fromOpt, untilOpt, loci, bufferedReads)
        }
      )
  }
}

object RegionRDD {
  private val rddMap = mutable.Map[Int, RegionRDD[_]]()
  implicit def rddToRegionRDD[R <: ReferenceRegion: ClassTag](
    rdd: RDD[R]
  )(
    implicit ordering: PartialOrdering[R]
  ): RegionRDD[R] =
    rddMap.getOrElseUpdate(
      rdd.id,
      new RegionRDD[R](rdd)
    ).asInstanceOf[RegionRDD[R]]
}
