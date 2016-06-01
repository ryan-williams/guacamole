package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.loci.set.{LociSet, TestInterval}
import org.hammerlab.guacamole.reference.{ReferencePosition, ReferenceRegion}
import org.hammerlab.guacamole.util.GuacFunSuite
import org.hammerlab.magic.iterator.RunLengthIterator

import scala.collection.SortedMap

case class TestRegion(contig: String, start: Long, end: Long) extends ReferenceRegion

class ContigWindowIteratorSuite extends GuacFunSuite with Util {

  def checkReads(
    halfWindowSize: Int,
    lociStr: String,
    intervals: Iterator[TestInterval]
  )(
    expected: ((String, Int), String)*
  ): Unit = {

//    val it =
//      ContigWindowIterator(
//        "chr1",
//        halfWindowSize,
//        LociSet(lociStr).onContig("chr1").iterator,
//        intervals,
//        skipEmpty = false
//      )
//
//    checkReads(it, expected.toMap)
    checkReads(
      new LociContigWindowIterator(
//        "chr1",
//        halfWindowSize,
        LociSet(lociStr).onContig("chr1").iterator,
        new ContigWindowIterator("chr1", halfWindowSize, intervals.buffered)
      ),
      expected.filter(_._2.nonEmpty).toMap
    )
  }

  test("hello world") {
    checkReads(
      halfWindowSize = 0,
      List(
        "chr1:99-103",
        "chr1:199-202"
      ).mkString(","),
      makeIntervals(
        (100, 200, 1),
        (101, 201, 1)
      )
    )(
      ("chr1",  99) -> "",
      ("chr1", 100) -> "[100,200)",
      ("chr1", 101) -> "[100,200), [101,201)",
      ("chr1", 102) -> "[100,200), [101,201)",
      ("chr1", 199) -> "[100,200), [101,201)",
      ("chr1", 200) -> "[101,201)",
      ("chr1", 201) -> ""
    )
  }

  test("simple reads, no window") {
    checkReads(
      halfWindowSize = 0,
      List(
        "chr1:99-103",
        "chr1:198-202",
        "chr1:299-302",
        "chr1:399-401"
      ).mkString(","),
      makeIntervals(
        (100, 200, 1),
        (101, 201, 1),
        (199, 299, 1),
        (200, 300, 1),
        (300, 400, 1)
      )
    )(
      ("chr1",  99) -> "",
      ("chr1", 100) -> "[100,200)",
      ("chr1", 101) -> "[100,200), [101,201)",
      ("chr1", 102) -> "[100,200), [101,201)",
      ("chr1", 198) -> "[100,200), [101,201)",
      ("chr1", 199) -> "[100,200), [101,201), [199,299)",
      ("chr1", 200) -> "[101,201), [199,299), [200,300)",
      ("chr1", 201) -> "[199,299), [200,300)",
      ("chr1", 299) -> "[200,300)",
      ("chr1", 300) -> "[300,400)",
      ("chr1", 301) -> "[300,400)",
      ("chr1", 399) -> "[300,400)",
      ("chr1", 400) -> ""
    )
  }

  test("simple reads, window 1") {
    checkReads(
      halfWindowSize = 1,
      List(
        "chr1:98-102",
        "chr1:197-203",
        "chr1:299-302",
        "chr1:400-402"
      ).mkString(","),
      makeIntervals(
        (100, 200, 1),
        (101, 201, 1),
        (199, 299, 1),
        (200, 300, 1),
        (300, 400, 1)
      )
    )(
      ("chr1",  98) -> "",
      ("chr1",  99) -> "[100,200)",
      ("chr1", 100) -> "[100,200), [101,201)",
      ("chr1", 101) -> "[100,200), [101,201)",
      ("chr1", 197) -> "[100,200), [101,201)",
      ("chr1", 198) -> "[100,200), [101,201), [199,299)",
      ("chr1", 199) -> "[100,200), [101,201), [199,299), [200,300)",
      ("chr1", 200) -> "[100,200), [101,201), [199,299), [200,300)",
      ("chr1", 201) -> "[101,201), [199,299), [200,300)",
      ("chr1", 202) -> "[199,299), [200,300)",
      ("chr1", 299) -> "[199,299), [200,300), [300,400)",
      ("chr1", 300) -> "[200,300), [300,400)",
      ("chr1", 301) -> "[300,400)",
      ("chr1", 400) -> "[300,400)",
      ("chr1", 401) -> ""
    )
  }

  test("contained reads") {
    checkReads(
      halfWindowSize = 1,
      List(
        "chr1:99-102",
        "chr1:148-150",
        "chr1:153-155",
        "chr1:160-162",
        "chr1:198-202",
        "chr1:255-257"
      ).mkString(","),
      makeIntervals(
        (100, 200, 1),
        (101, 199, 1),
        (102, 198, 1),
        (150, 160, 1),
        (155, 255, 1)
      )
    )(
      ("chr1",  99) -> "[100,200)",
      ("chr1", 100) -> "[100,200), [101,199)",
      ("chr1", 101) -> "[100,200), [101,199), [102,198)",
      ("chr1", 148) -> "[100,200), [101,199), [102,198)",
      ("chr1", 149) -> "[100,200), [101,199), [102,198), [150,160)",
      ("chr1", 153) -> "[100,200), [101,199), [102,198), [150,160)",
      ("chr1", 154) -> "[100,200), [101,199), [102,198), [150,160), [155,255)",
      ("chr1", 160) -> "[100,200), [101,199), [102,198), [150,160), [155,255)",
      ("chr1", 161) -> "[100,200), [101,199), [102,198), [155,255)",
      ("chr1", 198) -> "[100,200), [101,199), [102,198), [155,255)",
      ("chr1", 199) -> "[100,200), [101,199), [155,255)",
      ("chr1", 200) -> "[100,200), [155,255)",
      ("chr1", 201) -> "[155,255)",
      ("chr1", 255) -> "[155,255)",
      ("chr1", 256) -> ""
    )
  }

  test("many reads") {
    checkReads(
      halfWindowSize = 1,
      List(
        "chr1:98-100",
        "chr1:108-111",
        "chr1:119-122",
        "chr1:199-202"
      ).mkString(","),
      makeIntervals(
        (100, 200, 100000),
        (110, 120, 100000)
      )
    )(
      ("chr1",  98) -> "",
      ("chr1",  99) -> "[100,200)*100000",
      ("chr1", 108) -> "[100,200)*100000",
      ("chr1", 109) -> "[100,200)*100000, [110,120)*100000",
      ("chr1", 110) -> "[100,200)*100000, [110,120)*100000",
      ("chr1", 119) -> "[100,200)*100000, [110,120)*100000",
      ("chr1", 120) -> "[100,200)*100000, [110,120)*100000",
      ("chr1", 121) -> "[100,200)*100000",
      ("chr1", 199) -> "[100,200)*100000",
      ("chr1", 200) -> "[100,200)*100000",
      ("chr1", 201) -> ""
    )
  }

  test("skip gaps and all reads") {
    checkReads(
      halfWindowSize = 1,
      List(
        "chr1:50-52",
        "chr1:60-62",
        "chr1:150-152",
        "chr1:161-163"
      ).mkString(","),
      makeIntervals(
        (100, 110, 10),
        (120, 130, 10),
        (153, 160, 10)
      )
    )(
      ("chr1",  50) -> "",
      ("chr1",  51) -> "",
      ("chr1",  60) -> "",
      ("chr1",  61) -> "",
      ("chr1", 150) -> "",
      ("chr1", 151) -> "",
      ("chr1", 161) -> "",
      ("chr1", 162) -> ""
    )
  }

  test("skip gaps and some reads") {
    checkReads(
      halfWindowSize = 1,
      List(
        "chr1:50-52",
        "chr1:60-64",
        "chr1:150-153"
      ).mkString(","),
      makeIntervals(
        (62, 70, 10),
        (80, 90, 100)
      )
    )(
      ("chr1",  50) -> "",
      ("chr1",  51) -> "",
      ("chr1",  60) -> "",
      ("chr1",  61) -> "[62,70)*10",
      ("chr1",  62) -> "[62,70)*10",
      ("chr1",  63) -> "[62,70)*10",
      ("chr1", 150) -> "",
      ("chr1", 151) -> "",
      ("chr1", 152) -> ""
    )
  }

}
