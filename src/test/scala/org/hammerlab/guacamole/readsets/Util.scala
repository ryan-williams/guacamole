package org.hammerlab.guacamole.readsets

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.reference.{Contig, Interval, ReferencePosition, ReferenceRegion, TestInterval}
import org.hammerlab.magic.iterator.RunLengthIterator
import org.scalatest.Matchers

import scala.collection.{SortedMap, mutable}

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
    sc.parallelize(makeReads(reads).toSeq, numPartitions)

  def makeIntervals(intervals: Seq[(Int, Int, Int)]): BufferedIterator[TestInterval] =
    (for {
      (start, end, num) <- intervals
      i <- 0 until num
    } yield
      TestInterval(start, end)
    ).iterator.buffered

  def makeReads(contig: String, reads: (Int, Int, Int)*): Iterator[TestRegion] =
    makeReads((for { (start, end, num) <- reads } yield (contig, start, end, num)))

  def makeReads(reads: Seq[(String, Int, Int, Int)]): BufferedIterator[TestRegion] =
    (for {
      (contig, start, end, num) <- reads
      i <- 0 until num
    } yield
      TestRegion(contig, start, end)
    ).iterator.buffered

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

object Util {
  type TestPileup = (String, Long, String)

  // This is used as a closure in `RDD.map`s in ReadSetsSuite, so it's advantageous to keep it in its own object here
  // and not serialize ReadSetsSuite (which is not Serializable due to scalatest Matchers' `assertionsHelper`).
  def simplifyPileup(pileup: Pileup): TestPileup =
    (
      pileup.referenceName,
      pileup.locus,
      {
        val reads = mutable.Map[(Long, Long), Int]()
        for {
          e <- pileup.elements
          read = e.read
          contig = read.contig
          start = read.start
          end = read.end
        } {
          reads((start, end)) = reads.getOrElseUpdate((start, end), 0) + 1
        }

        (for {
          ((start, end), count) <- reads.toArray.sortBy(_._1._2)
        } yield
          s"[$start,$end)${if (count > 1) s"*$count" else ""}"
        ).mkString(", ")
      }
    )
}
