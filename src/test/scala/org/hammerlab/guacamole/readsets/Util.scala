package org.hammerlab.guacamole.readsets

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.{ReferencePosition, ReferenceRegion}
import org.hammerlab.magic.iterator.RunLengthIterator
import org.scalatest.Matchers

import scala.collection.SortedMap

trait Util extends Matchers {

  def sc: SparkContext

  def checkReads[R <: ReferenceRegion](pileups: Iterator[(ReferencePosition, Iterable[R])],
                                       expectedStrs: Map[(String, Int), String]): Unit = {

    val expected = expectedStrs.map(t => ReferencePosition(t._1._1, t._1._2) -> t._2)

    val actual: List[(ReferencePosition, String)] = windowIteratorStrings(pileups)
    val actualMap = SortedMap(actual: _*)
    for {
      (pos, str) <- actualMap
    } {
      withClue(s"$pos:") {
        str should be(expected.getOrElse(pos, ""))
      }
    }

    val missingLoci =
      expected
        .keys
        .filter(expected.keySet.diff(actualMap.keySet).contains)
        .toArray
        .sortBy(x => x)

    withClue("missing loci:") {
      missingLoci should be(Array())
    }
  }

  def makeReadsRDD(numPartitions: Int, reads: (String, Int, Int, Int)*): RDD[TestRegion] =
    sc.parallelize(makeReads(reads: _*).toSeq, numPartitions)

  def makeReads(contig: String, reads: (Int, Int, Int)*): Iterator[TestRegion] =
    makeReads((for { (start, end, num) <- reads } yield (contig, start, end, num)): _*)

  def makeReads(reads: (String, Int, Int, Int)*): Iterator[TestRegion] =
    (for {
      (contig, start, end, num) <- reads
      i <- 0 until num
    } yield
      TestRegion(contig, start, end)
    ).iterator

  def windowIteratorStrings[R <: ReferenceRegion](
    windowIterator: Iterator[(ReferencePosition, Iterable[R])]
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
