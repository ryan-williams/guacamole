package org.hammerlab.guacamole.loci.set

import org.hammerlab.guacamole.reference.ContigPosition._
import org.hammerlab.guacamole.reference.ReferencePosition

/**
 * An iterator over loci on a single contig. Loci from this iterator are sorted (monotonically increasing).
 *
 * This can be used as a plain scala Iterator[ReferencePosition], but also supports extra functionality for quickly
 * skipping ahead past a given locus.
 */
class ContigIterator(val contig: String, val loci: LociIterator)
  extends BufferedIterator[ReferencePosition] {

  override def head: ReferencePosition = ReferencePosition(contig, loci.head)

  override def hasNext: Boolean = loci.hasNext

  override def next(): ReferencePosition = ReferencePosition(contig, loci.next())

  def skipTo(nextPos: ReferencePosition): Unit = {
    if (nextPos.contig == contig) {
      loci.skipTo(nextPos.locus)
    }
  }
}

//object ContigIterator {
//  def apply(contig: Contig): ContigIterator =
//    ContigIterator(contig.name, contig.ranges.iterator.buffered)
//
//  def apply(contig: String, intervals: BufferedIterator[Interval]): ContigIterator =
//    new ContigIterator(contig, new LociIterator(intervals))
//}
