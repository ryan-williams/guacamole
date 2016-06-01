package org.hammerlab.guacamole.loci.set

import org.hammerlab.guacamole.reference.Interval
import org.hammerlab.guacamole.reference.ReferencePosition.Locus

class LociIterator(intervals: BufferedIterator[Interval]) extends BufferedIterator[Locus] {

  private var pos: Locus = -1L

  var stopAtOpt: Option[Locus] = None

  def advance(): Boolean = {
    if (!intervals.hasNext)
      return false

    if (pos == -1L)
      pos = intervals.head.start

    stopAtOpt.isEmpty || stopAtOpt.exists(pos < _)
  }

  override def head: Locus = {
    if (!advance()) throw new NoSuchElementException
    pos
  }

  /** true if calling next() will succeed. */
  override def hasNext: Boolean = advance()

  /**
   * Advance the iterator and return the current head.
   *
   * Throws NoSuchElementException if the iterator is already at the end.
   */
  def next(): Locus = {

    val ret = head

    pos += 1

    if (pos >= intervals.head.end) {
      intervals.next()
      pos = -1L
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
  def skipTo(locus: Long): this.type = {
    // Skip entire intervals until we hit one whose end is past the target locus.
    while (intervals.hasNext && intervals.head.end <= locus) {
      intervals.next()
    }

    // If we're not at the end of the iterator and the current head range includes the target locus, set our index
    // so that our next locus is the target.
    if (intervals.hasNext && intervals.head.start < locus && locus < intervals.head.end) {
      pos = locus
    }

    this
  }

  def stopAt(locus: Locus): this.type = {
    stopAtOpt = Some(locus)
    this
  }
}
