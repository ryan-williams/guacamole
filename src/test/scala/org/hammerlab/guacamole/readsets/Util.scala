package org.hammerlab.guacamole.readsets

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.reference.{Interval, ReferencePosition, ReferenceRegion, TestInterval}
import org.hammerlab.magic.iterator.RunLengthIterator
import org.scalatest.Matchers

import scala.collection.SortedMap

case class TestRegion(contig: String, start: Long, end: Long) extends ReferenceRegion

trait Util extends Matchers {

  def sc: SparkContext

  def checkReads[I <: Interval](pileups: Iterator[(ReferencePosition, Iterable[I])],
                                expectedStrs: Map[(String, Int), String]): Unit = {

    val expected = expectedStrs.map(t => ReferencePosition(t._1._1, t._1._2) -> t._2)

    val actual: List[(ReferencePosition, String)] = windowIteratorStrings(pileups)
    val actualMap = SortedMap(actual: _*)

    val extraLoci =
      (for {
        (k, v) <- actualMap
        if !expected.contains(k)
      } yield
        k -> v
      )
      .toArray
      .sortBy(x => x)

    val missingLoci =
      (for {
        (k, v) <- expected
        if !actualMap.contains(k)
      } yield
        k -> v
      )
      .toArray
      .sortBy(x => x)

    val msg =
      (
        List(
          s"expected ${expected.size} loci."
        ) ++
          (
            if (extraLoci.nonEmpty)
              List(
                s"${extraLoci.length} extra loci found:",
                s"\t${extraLoci.mkString("\n\t")}"
              )
            else
              Nil
          ) ++
          (
            if (missingLoci.nonEmpty)
              List(
                s"${missingLoci.length} missing loci:",
                s"\t${missingLoci.mkString("\n\t")}"
              )
            else
              Nil
          )
      ).mkString("\n")

    withClue(msg) {
      missingLoci.length should be(0)
      extraLoci.length should be(0)
    }
  }

  def makeReadsRDD(numPartitions: Int, reads: (String, Int, Int, Int)*): RDD[TestRegion] =
    sc.parallelize(makeReads(reads: _*).toSeq, numPartitions)

  def makeIntervals(intervals: (Int, Int, Int)*): Iterator[TestInterval] =
    (for {
      (start, end, num) <- intervals
      i <- 0 until num
    } yield
      TestInterval(start, end)
    ).iterator

  def makeReads(contig: String, reads: (Int, Int, Int)*): Iterator[TestRegion] =
    makeReads((for { (start, end, num) <- reads } yield (contig, start, end, num)): _*)

  def makeReads(reads: (String, Int, Int, Int)*): Iterator[TestRegion] =
    (for {
      (contig, start, end, num) <- reads
      i <- 0 until num
    } yield
      TestRegion(contig, start, end)
    ).iterator

  def windowIteratorStrings[I <: Interval](
    windowIterator: Iterator[(ReferencePosition, Iterable[I])]
  ): List[(ReferencePosition, String)] =
    (for {
      (pos, reads) <- windowIterator
    } yield {
      pos ->
        (for {
          (region, count) <- RunLengthIterator(reads.iterator)
        } yield {
          s"[${region.start},${region.end})${if (count > 1) s"*$count" else ""}"
        }).mkString(", ")
    }).toList

}
