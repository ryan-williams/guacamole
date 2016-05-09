package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.{Contig, ReferencePosition, ReferenceRegion}
import org.hammerlab.guacamole.util.GuacFunSuite

import scala.collection.{SortedMap, mutable}

case class TestRegion(contig: String, start: Long, end: Long) extends ReferenceRegion {
  override val referenceContig: Contig = contig
}

class WindowIteratorSuite extends GuacFunSuite {

  def checkReads(halfWindowSize: Int,
                 lociStr: String,
                 reads: Iterator[TestRegion],
                 expected: Map[Int, String]): Unit = {
    checkReads(
      windowIteratorStrings(
        WindowIterator(
          halfWindowSize,
          LociSet(lociStr),
          reads
        )
      ),
      expected
    )
  }

  def checkReads(actual: List[(Int, String)], expected: Map[Int, String]): Unit = {
    val actualMap = SortedMap(actual: _*)
    for {
      (pos, str) <- actualMap
    } {
      withClue(s"$pos") {
        str should be(expected.getOrElse(pos, ""))
      }
    }
  }

  def makeReads(contig: String, ranges: (Int, Int, Int)*): Iterator[TestRegion] =
    (for {
      (start, end, num) <- ranges
      i <- 0 until num
    } yield
      TestRegion(contig, start, end)
    ).iterator

  def windowIteratorStrings[R <: ReferenceRegion](windowIterator: WindowIterator[R]): List[(Int, String)] =
    (for {
      (pos, (reads, numDropped, numAdded)) <- windowIterator
    } yield {
      pos.pos.toInt ->
        reads.map(r => s"[${r.start},${r.end})").mkString(", ")
    }).toList

  test("simple") {
    checkReads(
      halfWindowSize = 0,
      "chr1:99-103",
      makeReads(
        "chr1",
        (100, 200, 1),
        (101, 201, 1)
      ),
      Map(
        99  -> "",
        100 -> "[100,200)",
        101 -> "[100,200), [101,201)",
        102 -> "[100,200), [101,201)"
      )
    )
  }

  test("simple 2") {
    checkReads(
      halfWindowSize = 0,
      "chr1:99-103,chr1:198-202,chr1:299-302,chr1:399-401",
      makeReads(
        "chr1",
        (100, 200, 1),
        (101, 201, 1),
        (199, 299, 1),
        (200, 300, 1),
        (300, 400, 1)
      ),
      Map(
        99  -> "",
        100 -> "[100,200)",
        101 -> "[100,200), [101,201)",
        102 -> "[100,200), [101,201)",
        198 -> "[100,200), [101,201)",
        199 -> "[100,200), [101,201), [199,299)",
        200 -> "[101,201), [199,299), [200,300)",
        201 -> "[101,201), [199,299), [200,300)",
        299 -> "[200,300)",
        300 -> "[300,400)",
        301 -> "[300,400)",
        399 -> "[300,400)",
        400 -> ""
      )
    )
  }

  test("simple 3") {
    checkReads(
      halfWindowSize = 1,
      "chr1:98-102,chr1:197-203,chr1:299-302,chr1:400-402",
      makeReads(
        "chr1",
        (100, 200, 1),
        (101, 201, 1),
        (199, 299, 1),
        (200, 300, 1),
        (300, 400, 1)
      ),
      Map(
        98  -> "",
        99  -> "[100,200)",
        100 -> "[100,200), [101,201)",
        101 -> "[100,200), [101,201)",
        197 -> "[100,200), [101,201)",
        198 -> "[100,200), [101,201), [199,299)",
        199 -> "[100,200), [101,201), [199,299), [200,300)",
        200 -> "[100,200), [101,201), [199,299), [200,300)",
        201 -> "[101,201), [199,299), [200,300)",
        202 -> "[199,299), [200,300)",
        299 -> "[199,299), [200,300)",
        300 -> "[200,300), [300,400)",
        301 -> "[300,400)",
        400 -> "[300,400)",
        401 -> ""
      )
    )
  }

}
