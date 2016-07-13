package org.hammerlab.guacamole.distributed

import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.util.TestUtil
import org.scalatest.{FunSuite, Matchers}

class RegionsByContigSuite extends FunSuite with Matchers {
  val readChr2  = TestUtil.makeRead("AAAAA", chr =  "chr2", start = 10)
  val readChr9  = TestUtil.makeRead("AAAAA", chr =  "chr9", start = 10)
  val readChr10 = TestUtil.makeRead("AAAAA", chr = "chr10", start = 10)

  test("1. chr2 and chr9: works, but chr10 empty iterator in wrong place") {

    val loci = LociSet("chr2:0-1000,chr9:0-1000,chr10:0-1000")

    val rbc = new RegionsByContig(Iterator(readChr2, readChr9))

    loci.contigs.map(contig => rbc.next(contig.name).toList).toList should be(
      List(
        List(), // chr10 :-\
        List(readChr2),
        List(readChr9)
      )
    )
  }

  test("2. chr2 and chr10; assertion failure") {
    val loci = LociSet("chr2:0-1000,chr9:0-1000,chr10:0-1000")

    val rbc = new RegionsByContig(Iterator(readChr2, readChr10))

    intercept[AssertionError]({
      loci.contigs.map(contig => rbc.next(contig.name).toList).toList
    }).getMessage should include("Regions are not sorted by contig")
  }

  test("3. chr2 and chr10; wrong result") {
    val loci = LociSet("chr2:0-1000,chr10:0-1000")

    val rbc = new RegionsByContig(Iterator(readChr2, readChr10))

    loci.contigs.map(contig => rbc.next(contig.name).toList).toList should be(
      List(
        List(),
        List(readChr2)
      )
    )
  }

  test("4. chr9 and chr10; chr10 reads missing") {
    val loci = LociSet("chr2:0-1000,chr9:0-1000,chr10:0-1000")

    val rbc = new RegionsByContig(Iterator(readChr9, readChr9, readChr10, readChr10))

    loci.contigs.map(contig => rbc.next(contig.name).toList).toList should be(
      List(
        List(),
        List(),
        List(readChr9, readChr9)
      )
    )
  }
}
