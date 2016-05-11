package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.{Contig, ReferencePosition, ReferenceRegion}
import org.hammerlab.guacamole.util.{GuacFunSuite, RunLengthIterator}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{SortedMap, mutable}

case class TestRegion(contig: String, start: Long, end: Long) extends ReferenceRegion {
  override val referenceContig: Contig = contig
}

class WindowIteratorSuite extends GuacFunSuite {

  def checkReads(halfWindowSize: Int,
                 lociStr: String,
                 reads: Iterator[TestRegion],
                 expected: Map[Int, (String, Int, Int)]): Unit = {
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

  def checkReads(actual: List[(Int, (String, Int, Int))], expected: Map[Int, (String, Int, Int)]): Unit = {
    val actualMap = SortedMap(actual: _*)
    for {
      (pos, (str, numDropped, numAdded)) <- actualMap
    } {
      withClue(s"$pos") {
        (str, numDropped, numAdded) should be(expected.getOrElse(pos, ("", -1, -1)))
      }
    }
    expected.size should be(actualMap.size)
  }

  def makeReads(contig: String, ranges: (Int, Int, Int)*): Iterator[TestRegion] =
    (for {
      (start, end, num) <- ranges
      i <- 0 until num
    } yield
      TestRegion(contig, start, end)
    ).iterator

  def windowIteratorStrings[R <: ReferenceRegion](windowIterator: WindowIterator[R]): List[(Int, (String, Int, Int))] =
    (for {
      (ReferencePosition(_, locus), (reads, numDropped, numAdded)) <- windowIterator
    } yield {
      locus.toInt ->
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

  test("simple") {
    checkReads(
      halfWindowSize = 0,
      "chr1:99-103,chr1:199-202",
      makeReads(
        "chr1",
        (100, 200, 1),
        (101, 201, 1)
      ),
      Map(
        99  -> ("", 0, 0),
        100 -> ("[100,200)", 0, 1),
        101 -> ("[100,200), [101,201)", 0, 1),
        102 -> ("[100,200), [101,201)", 0, 0),
        199 -> ("[100,200), [101,201)", 0, 0),
        200 -> ("[101,201)", 1, 0),
        201 -> ("", 1, 0)
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
        99  -> ("", 0, 0),
        100 -> ("[100,200)", 0, 1),
        101 -> ("[100,200), [101,201)", 0, 1),
        102 -> ("[100,200), [101,201)", 0, 0),
        198 -> ("[100,200), [101,201)", 0, 0),
        199 -> ("[100,200), [101,201), [199,299)", 0, 1),
        200 -> ("[101,201), [199,299), [200,300)", 1, 1),
        201 -> ("[199,299), [200,300)", 1, 0),
        299 -> ("[200,300)", 1, 0),
        300 -> ("[300,400)", 1, 1),
        301 -> ("[300,400)", 0, 0),
        399 -> ("[300,400)", 0, 0),
        400 -> ("", 1, 0)
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
        98  -> ("", 0, 0),
        99  -> ("[100,200)", 0, 1),
        100 -> ("[100,200), [101,201)", 0, 1),
        101 -> ("[100,200), [101,201)", 0, 0),
        197 -> ("[100,200), [101,201)", 0, 0),
        198 -> ("[100,200), [101,201), [199,299)", 0, 1),
        199 -> ("[100,200), [101,201), [199,299), [200,300)", 0, 1),
        200 -> ("[100,200), [101,201), [199,299), [200,300)", 0, 0),
        201 -> ("[101,201), [199,299), [200,300)", 1, 0),
        202 -> ("[199,299), [200,300)", 1, 0),
        299 -> ("[199,299), [200,300), [300,400)", 0, 1),
        300 -> ("[200,300), [300,400)", 1, 0),
        301 -> ("[300,400)", 1, 0),
        400 -> ("[300,400)", 0, 0),
        401 -> ("", 1, 0)
      )
    )
  }

  test("contained reads") {
    checkReads(
      halfWindowSize = 1,
      "chr1:99-102,chr1:148-150,chr1:153-155,chr1:160-162,chr1:198-202,chr1:255-257",
      makeReads(
        "chr1",
        (100, 200, 1),
        (101, 199, 1),
        (102, 198, 1),
        (150, 160, 1),
        (155, 255, 1)
      ),
      Map(
        99  -> ("[100,200)", 0, 1),
        100 -> ("[100,200), [101,199)", 0, 1),
        101 -> ("[100,200), [101,199), [102,198)", 0, 1),
        148 -> ("[100,200), [101,199), [102,198)", 0, 0),
        149 -> ("[100,200), [101,199), [102,198), [150,160)", 0, 1),
        153 -> ("[100,200), [101,199), [102,198), [150,160)", 0, 0),
        154 -> ("[100,200), [101,199), [102,198), [150,160), [155,255)", 0, 1),
        160 -> ("[100,200), [101,199), [102,198), [150,160), [155,255)", 0, 0),
        161 -> ("[100,200), [101,199), [102,198), [155,255)", 1, 0),
        198 -> ("[100,200), [101,199), [102,198), [155,255)", 0, 0),
        199 -> ("[100,200), [101,199), [155,255)", 1, 0),
        200 -> ("[100,200), [155,255)", 1, 0),
        201 -> ("[155,255)", 1, 0),
        255 -> ("[155,255)", 0, 0),
        256 -> ("", 1, 0)
      )
    )
  }

  test("many reads") {
    checkReads(
      halfWindowSize = 1,
      "chr1:98-100,chr1:108-111,chr1:119-122,chr1:199-202",
      makeReads(
        "chr1",
        (100, 200, 100000),
        (110, 120, 100000)
      ),
      Map(
        98  -> ("", 0, 0),
        99  -> ("[100,200)*100000", 0, 100000),
        108 -> ("[100,200)*100000", 0, 0),
        109 -> ("[100,200)*100000, [110,120)*100000", 0, 100000),
        110 -> ("[100,200)*100000, [110,120)*100000", 0, 0),
        119 -> ("[100,200)*100000, [110,120)*100000", 0, 0),
        120 -> ("[100,200)*100000, [110,120)*100000", 0, 0),
        121 -> ("[100,200)*100000", 100000, 0),
        199 -> ("[100,200)*100000", 0, 0),
        200 -> ("[100,200)*100000", 0, 0),
        201 -> ("", 100000, 0)
      )
    )
  }

}
