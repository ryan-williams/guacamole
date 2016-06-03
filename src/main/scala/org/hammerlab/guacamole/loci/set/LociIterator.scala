package org.hammerlab.guacamole.loci.set

import org.hammerlab.guacamole.reference.Interval
import org.hammerlab.guacamole.reference.ReferencePosition.Locus
import org.hammerlab.guacamole.util.OptionIterator

class LociIterator(intervals: BufferedIterator[Interval]) extends OptionIterator[Locus] {

  private var nextPos: Option[Locus] = None

  var stopAtOpt: Option[Locus] = None

  override def postNext(n: Locus): Unit = {
    nextPos = Some(n + 1)
  }

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
  def skipTo(locus: Locus): this.type = {
    if (nextPos.isEmpty || nextPos.exists(_ < locus)) {
      nextPos = Some(locus)
      _next = None
    }

    this
  }

  def stopAt(locus: Locus): this.type = {
    stopAtOpt = Some(locus)
    this
  }
}
