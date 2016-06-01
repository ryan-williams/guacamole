package org.hammerlab.guacamole.loci.set

import java.lang.{Long => JLong}

import com.google.common.collect.{Range => JRange}
import org.hammerlab.guacamole.reference.{Interval, ReferencePosition}
import org.scalatest.{FunSuite, Matchers}

case class TestInterval(start: Long, end: Long) extends Interval

class LociIteratorSuite extends FunSuite with Matchers {

  def loci(intervals: (Int, Int)*): LociIterator =
    new LociIterator(
      (for {
        (start, end) <- intervals
      } yield
        TestInterval(start, end)
      ).iterator.buffered
    )

  test("simple") {
    loci(100 -> 110).toList should be(100 until 110)
  }

  test("skipTo") {
    val it = loci(100 -> 110)
    it.skipTo(103)
    it.head should be(103)
    it.toList should be(103 until 110)
  }

  test("intervals") {
    loci(100 -> 110, 120 -> 130).toList should be((100 until 110) ++ (120 until 130))
  }

  test("stop at beginning") {
    loci(100 -> 110).stopAt(100).toList should be(Nil)
  }

  test("stop before beginning") {
    loci(100 -> 110).stopAt(99).toList should be(Nil)
  }

  test("stop 1 after beginning") {
    loci(100 -> 110).stopAt(101).toList should be(List(100))
  }

  test("stop 2 after beginning") {
    loci(100 -> 110).stopAt(102).toList should be(List(100, 101))
  }

  test("stop 2 before end") {
    loci(100 -> 110).stopAt(108).toList should be(100 until 108)
  }

  test("stop 1 before end") {
    loci(100 -> 110).stopAt(109).toList should be(100 until 109)
  }

  test("stop at end") {
    loci(100 -> 110).stopAt(110).toList should be(100 until 110)
  }

  test("stop after end") {
    loci(100 -> 110).stopAt(111).toList should be(100 until 110)
  }

  test("stop within first range") {
    loci(100 -> 110, 120 -> 130).stopAt(105).toList should be(100 until 105)
  }

  test("stop within second range") {
    loci(100 -> 110, 120 -> 130).stopAt(125).toList should be((100 until 110) ++ (120 until 125))
  }

  test("stop at start of second range") {
    loci(100 -> 110, 120 -> 130).stopAt(120).toList should be(100 until 110)
  }

  test("stop between ranges") {
    loci(100 -> 110, 120 -> 130).stopAt(115).toList should be(100 until 110)
  }

  test("stop between adjacent ranges") {
    loci(100 -> 110, 110 -> 120).stopAt(110).toList should be(100 until 110)
  }

  test("equal start and stop") {
    loci(100 -> 110, 120 -> 130).skipTo(108).stopAt(108).toList should be(Nil)
  }

  test("start and stop in first range") {
    loci(100 -> 110, 120 -> 130).skipTo(102).stopAt(108).toList should be(102 until 108)
  }

  test("start and stop across ranges") {
    loci(100 -> 110, 120 -> 130).skipTo(108).stopAt(122).toList should be(List(108, 109, 120, 121))
  }

  test("start and stop in second range") {
    loci(100 -> 110, 120 -> 130).skipTo(121).stopAt(129).toList should be(121 until 129)
  }

}
