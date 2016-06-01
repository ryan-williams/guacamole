package org.hammerlab.guacamole.loci.set

import org.hammerlab.guacamole.reference.Interval
import org.hammerlab.guacamole.reference.ReferencePosition.Locus
import org.hammerlab.guacamole.util.OptionIterator

class LociIterator(intervals: BufferedIterator[Interval]) extends OptionIterator[Locus] {

  private var nextPos: Option[Locus] = None

  var stopAtOpt: Option[Locus] = None

//  override def hasNext: Boolean = {
//    if (!intervals.hasNext) false
//    else if (pos == -1L)
//      pos = intervals.head.start
//
//    stopAtOpt.isEmpty || stopAtOpt.exists(pos < _)
//  }
//
//  override def head: Locus = {
//    if (!hasNext) throw new NoSuchElementException
//    pos
//  }

  override def postNext(n: Locus): Unit = {
    nextPos = Some(n + 1)
  }

  /**
   * Advance the iterator and return the current head.
   *
   * Throws NoSuchElementException if the iterator is already at the end.
   */
//  def next(): Locus = {
//
//    val ret = head
//
//    skipTo(pos + 1)
//
//    ret
//  }

  override def _advance: Option[Locus] = {
    if (!intervals.hasNext)
      None
    else
      nextPos match {
        case Some(n) if stopAtOpt.exists(_ <= n) =>
          None
        case Some(n) if intervals.head.contains(n) =>
          nextPos
        case Some(n) if intervals.head.end <= n =>
          intervals.next()
          _advance
        case _ =>
          if (stopAtOpt.exists(_ <= intervals.head.start))
            None
          else
            Some(intervals.head.start)
      }
  }

  /**
   * Skip ahead to the first locus in the iterator that is at or past the given locus.
   *
   * After calling this, a subsequent call to next() will return the first element in the iterator that is >= the
   * given locus. If there is no such element, then the iterator will be empty after calling this method.
   *
   */
  def skipTo(locus: Long): this.type = {
    if (nextPos.isEmpty || nextPos.exists(_ < locus)) {
      nextPos = Some(locus)
      _next = None
    }
//    // Skip entire intervals until we hit one whose end is past the target locus.
//    while (intervals.hasNext && intervals.head.end <= locus) {
//      intervals.next()
//    }
//
//    // If we're not at the end of the iterator and the current head range includes the target locus, set our index
//    // so that our next locus is the target.
//    if (intervals.hasNext)
//      if (intervals.head.start <= locus && locus < intervals.head.end)
//        pos = locus
//      else {
//        intervals.next()
//      }
//
//
    this
  }

  def stopAt(locus: Locus): this.type = {
    stopAtOpt = Some(locus)
    this
  }
}
