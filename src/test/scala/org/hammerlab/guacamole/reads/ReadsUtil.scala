package org.hammerlab.guacamole.reads

import org.hammerlab.guacamole.readsets.rdd.TestRegion
import org.scalatest.Matchers

trait ReadsUtil extends Matchers {
  def makeReads(contig: String, reads: (Int, Int, Int)*): Iterator[TestRegion] =
    makeReads((for { (start, end, num) <- reads } yield (contig, start, end, num)))

  def makeReads(reads: Seq[(String, Int, Int, Int)]): BufferedIterator[TestRegion] =
    (for {
      (contig, start, end, num) <- reads
      i <- 0 until num
    } yield
      TestRegion(contig, start, end)
    ).iterator.buffered
}
