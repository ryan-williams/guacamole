package org.hammerlab.guacamole.loci.set

import java.lang.{Long => JLong}

import com.google.common.collect.{Range => JRange}
import org.hammerlab.guacamole.reference.ReferencePosition
import org.scalatest.{FunSuite, Matchers}

class ContigIteratorSuite extends FunSuite with Matchers {

  def contig(name: String, ranges: (Int, Int)*): Contig =
    Contig(
      name,
      for {
        (l, u) <- ranges
      } yield
        JRange.closedOpen(l.toLong: JLong, u.toLong: JLong)
    )

  test("simple") {
    val it = ContigIterator(contig("chr1", (100, 110)))
    it.toList should be(
      List(
        ReferencePosition("chr1", 100),
        ReferencePosition("chr1", 101),
        ReferencePosition("chr1", 102),
        ReferencePosition("chr1", 103),
        ReferencePosition("chr1", 104),
        ReferencePosition("chr1", 105),
        ReferencePosition("chr1", 106),
        ReferencePosition("chr1", 107),
        ReferencePosition("chr1", 108),
        ReferencePosition("chr1", 109)
      )
    )
  }

  test("skipTo") {
    val it = ContigIterator(contig("chr1", (100, 110)))
    it.skipTo(103)
    it.toList should be(
      List(
        ReferencePosition("chr1", 103),
        ReferencePosition("chr1", 104),
        ReferencePosition("chr1", 105),
        ReferencePosition("chr1", 106),
        ReferencePosition("chr1", 107),
        ReferencePosition("chr1", 108),
        ReferencePosition("chr1", 109)
      )
    )
  }
}
