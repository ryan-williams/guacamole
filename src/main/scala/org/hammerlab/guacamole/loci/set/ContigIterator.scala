package org.hammerlab.guacamole.loci.set

import org.hammerlab.guacamole.reference.{Position, Contig => ReferenceContig}

/**
 * An iterator over loci on a single contig. Loci from this iterator are sorted (monotonically increasing).
 *
 * This can be used as a plain scala Iterator[ReferencePosition], but also supports extra functionality for quickly
 * skipping ahead past a given locus.
 */
class ContigIterator(val contig: ReferenceContig, val loci: LociIterator)
  extends BufferedIterator[Position] {

  override def head: Position = Position(contig, loci.head)

  override def hasNext: Boolean = loci.hasNext

  override def next(): Position = Position(contig, loci.next())

  def skipTo(nextPos: Position): Unit = {
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
