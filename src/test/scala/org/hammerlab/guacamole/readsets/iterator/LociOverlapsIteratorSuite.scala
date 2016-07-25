package org.hammerlab.guacamole.readsets.iterator

import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reads.ReadsUtil
import org.hammerlab.guacamole.readsets.iterator.overlaps.LociOverlapsIterator
import org.hammerlab.guacamole.reference.{Interval, IntervalsUtil, Position}
import org.hammerlab.magic.iterator.RunLengthIterator
import org.scalatest.{FunSuite, Matchers}

import scala.collection.SortedMap
import scala.reflect.ClassTag

class LociOverlapsIteratorSuite
  extends FunSuite
    with Matchers
    with ReadsUtil
    with IntervalsUtil {

  def checkReads(
    halfWindowSize: Int,
    loci: String
  )(
    intervals: (Int, Int, Int)*
  )(
    expected: (Int, String)*
  ): Unit = {

    val contig = "chr1"

    val it =
      LociOverlapsIterator(
        halfWindowSize,
        makeIntervals(intervals)
      )
      .intersect(
        LociSet(loci).onContig("chr1").iterator
      )

    checkReads(
      for {
        (locus, reads) <- it
      } yield
        Position(contig, locus) -> reads,
      (for {
        (locus, str) <- expected
      } yield
        contig -> locus -> str
      ).toMap
    )
  }

  def checkReads[I <: Interval: ClassTag](pileups: Iterator[(Position, Iterable[I])],
                                          expectedStrs: Map[(String, Int), String]): Unit = {

    val expected = expectedStrs.map(t => Position(t._1._1, t._1._2) -> t._2)

    val actual: List[(Position, String)] = windowIteratorStrings(pileups)
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

    val incorrectLoci =
      (for {
        (k, e) <- expected
        a <- actualMap.get(k)
        if a != e
      } yield
        k -> (a, e)
        )
      .toArray
      .sortBy(_._1)

    val incorrectLociStr =
      (for {
        (k, (a, e)) <- incorrectLoci
      } yield
        s"$k:\tactual: $a\texpected: $e"
        ).mkString("\n")

    withClue(s"differing loci:\n$incorrectLociStr") {
      incorrectLoci.length should be(0)
    }
  }

  def windowIteratorStrings[I <: Interval: ClassTag](
    windowIterator: Iterator[(Position, Iterable[I])]): List[(Position, String)] =

    (for {
      (pos, reads) <- windowIterator
    } yield {
      pos ->
        (for {
          (region, count) <- RunLengthIterator(reads.toArray.sortBy(_.start).iterator)
        } yield {
          s"[${region.start},${region.end})${if (count > 1) s"*$count" else ""}"
        }).mkString(", ")
    }).toList

  test("hello world") {
    checkReads(
      halfWindowSize = 0,
      loci = List(
        "chr1:99-103",
        "chr1:199-202"
      ).mkString(",")
    )(
      (100, 200, 1),
      (101, 201, 1)
    )(
      100 -> "[100,200)",
      101 -> "[100,200), [101,201)",
      102 -> "[100,200), [101,201)",
      199 -> "[100,200), [101,201)",
      200 -> "[101,201)"
    )
  }

  test("simple reads, no window") {
    checkReads(
      halfWindowSize = 0,
      loci = List(
        "chr1:99-103",
        "chr1:198-202",
        "chr1:299-302",
        "chr1:399-401"
      ).mkString(",")
    )(
      (100, 200, 1),
      (101, 201, 1),
      (199, 299, 1),
      (200, 300, 1),
      (300, 400, 1)
    )(
      100 -> "[100,200)",
      101 -> "[100,200), [101,201)",
      102 -> "[100,200), [101,201)",
      198 -> "[100,200), [101,201)",
      199 -> "[100,200), [101,201), [199,299)",
      200 -> "[101,201), [199,299), [200,300)",
      201 -> "[199,299), [200,300)",
      299 -> "[200,300)",
      300 -> "[300,400)",
      301 -> "[300,400)",
      399 -> "[300,400)"
    )
  }

  test("simple reads, window 1") {
    checkReads(
      halfWindowSize = 1,
      loci = List(
        "chr1:98-102",
        "chr1:197-203",
        "chr1:299-302",
        "chr1:400-402"
      ).mkString(",")
    )(
      (100, 200, 1),
      (101, 201, 1),
      (199, 299, 1),
      (200, 300, 1),
      (300, 400, 1)
    )(
       99 -> "[100,200)",
      100 -> "[100,200), [101,201)",
      101 -> "[100,200), [101,201)",
      197 -> "[100,200), [101,201)",
      198 -> "[100,200), [101,201), [199,299)",
      199 -> "[100,200), [101,201), [199,299), [200,300)",
      200 -> "[100,200), [101,201), [199,299), [200,300)",
      201 -> "[101,201), [199,299), [200,300)",
      202 -> "[199,299), [200,300)",
      299 -> "[199,299), [200,300), [300,400)",
      300 -> "[200,300), [300,400)",
      301 -> "[300,400)",
      400 -> "[300,400)"
    )
  }

  test("contained reads") {
    checkReads(
      halfWindowSize = 1,
      loci = List(
        "chr1:99-102",
        "chr1:148-150",
        "chr1:153-155",
        "chr1:160-162",
        "chr1:198-202",
        "chr1:255-257"
      ).mkString(",")
    )(
      (100, 200, 1),
      (101, 199, 1),
      (102, 198, 1),
      (150, 160, 1),
      (155, 255, 1)
    )(
       99 -> "[100,200)",
      100 -> "[100,200), [101,199)",
      101 -> "[100,200), [101,199), [102,198)",
      148 -> "[100,200), [101,199), [102,198)",
      149 -> "[100,200), [101,199), [102,198), [150,160)",
      153 -> "[100,200), [101,199), [102,198), [150,160)",
      154 -> "[100,200), [101,199), [102,198), [150,160), [155,255)",
      160 -> "[100,200), [101,199), [102,198), [150,160), [155,255)",
      161 -> "[100,200), [101,199), [102,198), [155,255)",
      198 -> "[100,200), [101,199), [102,198), [155,255)",
      199 -> "[100,200), [101,199), [155,255)",
      200 -> "[100,200), [155,255)",
      201 -> "[155,255)",
      255 -> "[155,255)"
    )
  }

  test("many reads") {
    checkReads(
      halfWindowSize = 1,
      loci = List(
        "chr1:98-100",
        "chr1:108-111",
        "chr1:119-122",
        "chr1:199-202"
      ).mkString(",")
    )(
      (100, 200, 100000),
      (110, 120, 100000)
    )(
       99 -> "[100,200)*100000",
      108 -> "[100,200)*100000",
      109 -> "[100,200)*100000, [110,120)*100000",
      110 -> "[100,200)*100000, [110,120)*100000",
      119 -> "[100,200)*100000, [110,120)*100000",
      120 -> "[100,200)*100000, [110,120)*100000",
      121 -> "[100,200)*100000",
      199 -> "[100,200)*100000",
      200 -> "[100,200)*100000"
    )
  }

  test("skip gaps and all reads") {
    checkReads(
      halfWindowSize = 1,
      loci = List(
        "chr1:50-52",
        "chr1:60-62",
        "chr1:150-152",
        "chr1:161-163"
      ).mkString(",")
    )(
      (100, 110, 10),
      (120, 130, 10),
      (153, 160, 10)
    )()
  }

  test("skip gaps and some reads") {
    checkReads(
      halfWindowSize = 1,
      loci = List(
        "chr1:50-52",
        "chr1:60-64",
        "chr1:150-153"
      ).mkString(",")
    )(
      (62, 70, 10),
      (80, 90, 100)
    )(
      61 -> "[62,70)*10",
      62 -> "[62,70)*10",
      63 -> "[62,70)*10"
    )
  }
}
