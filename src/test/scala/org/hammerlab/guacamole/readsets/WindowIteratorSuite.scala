package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.{ReferencePosition, ReferenceRegion}
import org.hammerlab.guacamole.util.{GuacFunSuite, RunLengthIterator}
import org.scalatest.Matchers

import scala.collection.SortedMap

class WindowIteratorSuite extends GuacFunSuite with Matchers {
  def checkReads(halfWindowSize: Int,
                 lociStr: String,
                 reads: Iterator[TestRegion],
                 expected: Map[(String, Int), (String, Int, Int)]/*,
                 expectedQueueSizes: (Int, Int) = (0, 0)*/): Unit = {
    val it =
      new WindowIterator(
        halfWindowSize,
        None,
        None,
        LociSet(lociStr),
        reads.buffered
      )

    checkReads(
      windowIteratorStrings(it),
      expected.map(t => ReferencePosition(t._1._1, t._1._2) -> t._2)
    )

//    it.ends.size should be(0)
//    it.queue.size should be(0)
  }

  def checkReads(actual: List[(ReferencePosition, (String, Int, Int))],
                 expected: Map[ReferencePosition, (String, Int, Int)]): Unit = {
    val actualMap = SortedMap(actual: _*)
    for {
      (pos, (str, numDropped, numAdded)) <- actualMap
    } {
      withClue(s"$pos:") {
        (str, numDropped, numAdded) should be(expected.getOrElse(pos, ("", -1, -1)))
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

  def makeReads(reads: (String, Int, Int, Int)*): Iterator[TestRegion] =
    (for {
      (contig, start, end, num) <- reads
      i <- 0 until num
    } yield
      TestRegion(contig, start, end)
    ).iterator

  def windowIteratorStrings[R <: ReferenceRegion](windowIterator: WindowIterator[R]): List[(ReferencePosition, (String, Int, Int))] =
    (for {
      (pos, (reads, numDropped, numAdded)) <- windowIterator
    } yield {
      pos ->
        (
          (for {
            (region, count) <- RunLengthIterator(reads.iterator)
          } yield {
            s"[${region.start},${region.end})${if (count > 1) s"*$count" else ""}"
          }).mkString(", "),
          numDropped,
          numAdded
        )
    }).toList

  test("hello world") {
    checkReads(
      halfWindowSize = 1,
      "chr1:50-52,chr1:98-102,chr1:199-203,chr2:10-12,chr2:100-102,chr4:10-12,chr5:100-102",
      makeReads(
        ("chr1", 100, 200,  1),
        ("chr1", 101, 201,  1),
        ("chr2",   8,   9,  1),
        ("chr2",  13,  15,  1),
        ("chr2",  90, 100,  1),
        ("chr2", 102, 105,  1),
        ("chr2", 103, 106,  1),
        ("chr2", 110, 120,  1),
        ("chr3", 100, 200,  1),
        ("chr5",  90, 110, 10)
      ),
      Map(
        ("chr1",  50) -> ("", 0, 0),
        ("chr1",  51) -> ("", 0, 0),
        ("chr1",  98) -> ("", 0, 0),
        ("chr1",  99) -> ("[100,200)", 0, 1),
        ("chr1", 100) -> ("[100,200), [101,201)", 0, 1),
        ("chr1", 101) -> ("[100,200), [101,201)", 0, 0),
        ("chr1", 199) -> ("[100,200), [101,201)", 0, 0),
        ("chr1", 200) -> ("[100,200), [101,201)", 0, 0),
        ("chr1", 201) -> ("[101,201)", 1, 0),
        ("chr1", 202) -> ("", 1, 0),
        ("chr2",  10) -> ("", 0, 0),
        ("chr2",  11) -> ("", 0, 0),
        ("chr2", 100) -> ( "[90,100)", 0, 1),
        ("chr2", 101) -> ("[102,105)", 1, 1),
        ("chr5", 100) -> ("[90,110)*10", 0, 10),
        ("chr5", 101) -> ("[90,110)*10", 0,  0)
      )
    )
  }

}
