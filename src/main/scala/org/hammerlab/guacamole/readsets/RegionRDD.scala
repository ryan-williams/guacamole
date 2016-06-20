package org.hammerlab.guacamole.readsets

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.loci.Coverage.PositionCoverage
import org.hammerlab.guacamole.loci.set.{LociSet, TakeLociIterator}
import org.hammerlab.guacamole.reference.ReferencePosition.NumLoci
import org.hammerlab.guacamole.reference.{Contig, ReferencePosition, ReferenceRegion}
import org.hammerlab.magic.rdd.RDDStats._
import org.hammerlab.magic.rdd.RunLengthRDD._
import org.hammerlab.magic.util.Stats

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class RegionRDD[R <: ReferenceRegion: ClassTag](@transient rdd: RDD[R])
  extends Serializable {

  @transient val sc = rdd.sparkContext

  @transient lazy val (
    numEmptyPartitions: Int,
    numPartitionsSpanningContigs: Int,
    partitionSpans: ArrayBuffer[Long],
    spanStats: Stats[NumLoci],
    nonEmptySpanStats: Stats[NumLoci]
  ) = {
    val partitionBounds = rdd.partitionBounds
    var numEmpty = 0
    var numCrossingContigs = 0
    val spans = ArrayBuffer[NumLoci]()
    val nonEmptySpans = ArrayBuffer[NumLoci]()

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

  def shuffleCoverage(halfWindowSize: Int, contigLengthsBroadcast: Broadcast[ContigLengths]): RDD[PositionCoverage] = {
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

  @transient val coverages_ = mutable.Map[(Int, LociSet), RDD[PositionCoverage]]()
  def coverage(halfWindowSize: Int, loci: LociSet): RDD[PositionCoverage] = coverage(halfWindowSize, sc.broadcast(loci))
  def coverage(halfWindowSize: Int, lociBroadcast: Broadcast[LociSet]): RDD[PositionCoverage] =
    coverages_.getOrElseUpdate(
      (halfWindowSize, lociBroadcast.value),
      {
        rdd
          .mapPartitions(it => {
            val loci = lociBroadcast.value
            for {
              (contigRegionsIterator, contigLociIterator) <- new ContigsIterator(it.buffered, loci)
              contig = contigRegionsIterator.contig
              lociContig = loci.onContig(contig)
              coverage <- ContigCoverageIterator(halfWindowSize, contigRegionsIterator, contigLociIterator)
            } yield
              coverage
          })
          .reduceByKey(_ + _)
          .sortByKey()
      }
    )

  @transient val depths_ = mutable.Map[(Int, LociSet), RDD[(Int, ReferencePosition)]]()
  def depths(halfWindowSize: Int, loci: LociSet): RDD[(Int, ReferencePosition)] =
    depths_.getOrElseUpdate(
      halfWindowSize -> loci,
      coverage(halfWindowSize, loci).map(t => t._2.depth -> t._1).sortByKey(ascending = false)
    )

  @transient val starts_ = mutable.Map[(Int, LociSet), RDD[(Int, ReferencePosition)]]()
  def starts(halfWindowSize: Int, loci: LociSet): RDD[(Int, ReferencePosition)] =
    starts_.getOrElseUpdate(
      halfWindowSize -> loci,
      coverage(halfWindowSize, loci).map(t => t._2.starts -> t._1).sortByKey(ascending = false)
    )

  @transient val ends_ = mutable.Map[(Int, LociSet), RDD[(Int, ReferencePosition)]]()
  def ends(halfWindowSize: Int, loci: LociSet): RDD[(Int, ReferencePosition)] =
    ends_.getOrElseUpdate(
      halfWindowSize -> loci,
      coverage(halfWindowSize, loci).map(t => t._2.ends -> t._1).sortByKey(ascending = false)
    )

  def partitionDepths(halfWindowSize: Int, loci: LociSet, depthCutoff: Int): RDD[((Contig, Boolean), Int)] = {
    (for {
      (ReferencePosition(contig, _), Coverage(depth, _, _)) <- coverage(halfWindowSize, loci)
    } yield
      contig -> (depth <= depthCutoff)
    ).runLengthEncode
  }

  def makeCappedLociSets(halfWindowSize: Int,
                         loci: LociSet,
                         maxRegionsPerPartition: Int): RDD[LociSet] =
    coverage(halfWindowSize, sc.broadcast(loci)).mapPartitionsWithIndex((idx, it) =>
      new TakeLociIterator(it.buffered, maxRegionsPerPartition)
    )
}

object RegionRDD {
  private val rddMap = mutable.Map[Int, RegionRDD[_]]()
  implicit def rddToRegionRDD[R <: ReferenceRegion: ClassTag](rdd: RDD[R]): RegionRDD[R] =
    rddMap.getOrElseUpdate(
      rdd.id,
      new RegionRDD[R](rdd)
    ).asInstanceOf[RegionRDD[R]]

  def validLociCounts(depthRuns: RDD[((Contig, Boolean), Int)]): (NumLoci, NumLoci) = {
    val map =
      (for {
        ((_, validDepth), numLoci) <- depthRuns
      } yield
        validDepth -> numLoci.toLong
      )
      .reduceByKey(_ + _)
      .collectAsMap

    (map.getOrElse(true, 0), map.getOrElse(false, 0))
  }
}
