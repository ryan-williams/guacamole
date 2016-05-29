package org.hammerlab.guacamole.loci.set

import org.hammerlab.guacamole.loci.SimpleRange
import org.hammerlab.guacamole.reference.ReferencePosition

/**
 * An iterator over loci on a single contig. Loci from this iterator are sorted (monotonically increasing).
 *
 * This can be used as a plain scala Iterator[ReferencePosition], but also supports extra functionality for quickly
 * skipping ahead past a given locus.
 */
class ContigIterator private(contigName: String, ranges: BufferedIterator[SimpleRange])
  extends BufferedIterator[ReferencePosition] {

  override def head: ReferencePosition = {
    if (pos == null) {
      pos = ReferencePosition(contigName, ranges.head.start)
    }

    pos
  }

  /** true if calling next() will succeed. */
  override def hasNext: Boolean = ranges.hasNext

  private var pos: ReferencePosition = _

  /**
   * Advance the iterator and return the current head.
   *
   * Throws NoSuchElementException if the iterator is already at the end.
   */
  def next(): ReferencePosition = {

    val ret = head

    pos += 1

    if (pos.end > ranges.head.end) {
      ranges.next()
      pos = null
    }

    ret
  }

  /**
   * Skip ahead to the first locus in the iterator that is at or past the given locus.
   *
   * After calling this, a subsequent call to next() will return the first element in the iterator that is >= the
   * given locus. If there is no such element, then the iterator will be empty after calling this method.
   *
   */
  def skipTo(locus: Long): Unit = {
    // Skip entire ranges until we hit one whose end is past the target locus.
    while (ranges.hasNext && ranges.head.end <= locus) {
      ranges.next()
    }

    // If we're not at the end of the iterator and the current head range includes the target locus, set our index
    // so that our next locus is the target.
    if (ranges.hasNext && ranges.head.start < locus && locus < ranges.head.end) {
      pos = ReferencePosition(contigName, locus)
    }
  }

  def skipTo(nextPos: ReferencePosition): Unit = {
    if (nextPos.contig == contigName) {
      skipTo(nextPos.locus)
    }
  }
}

object ContigIterator {
  def apply(contig: Contig): ContigIterator = new ContigIterator(contig.name, contig.ranges.iterator.buffered)
}
