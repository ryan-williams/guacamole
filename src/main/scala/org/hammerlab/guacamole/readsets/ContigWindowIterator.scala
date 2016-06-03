package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.reference.ReferencePosition.Locus
import org.hammerlab.guacamole.reference.{HasLocus, Interval}

import scala.collection.mutable

case class LociIntervals[I <: Interval](locus: Locus, intervals: Iterable[I]) extends HasLocus

class ContigWindowIterator[I <: Interval](halfWindowSize: Int, regions: BufferedIterator[I])
  extends SkippableLociIterator[LociIntervals[I]] {

  private val queue = new mutable.PriorityQueue[I]()(Interval.orderByEnd[I])

  override def _advance: Option[LociIntervals[I]] = {
    updateQueue()

    if (queue.isEmpty) {
      if (!regions.hasNext)
        return None

      locus = regions.head.start - halfWindowSize
      return _advance
    }

    Some(LociIntervals(locus, queue))
  }

  def updateQueue(): Unit = {
    val lowerLimit = locus - halfWindowSize
    val upperLimit = locus + halfWindowSize

    while (queue.headOption.exists(_.end <= lowerLimit)) {
      queue.dequeue
    }

    while (regions.hasNext && regions.head.start <= upperLimit) {
      if (regions.head.end > lowerLimit) {
        queue.enqueue(regions.head)
      }
      regions.next()
    }
  }

}
