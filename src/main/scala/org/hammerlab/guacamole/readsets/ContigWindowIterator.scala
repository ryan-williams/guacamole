package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.reference.ReferencePosition.Locus
import org.hammerlab.guacamole.reference.{Contig, Interval, ReferencePosition}

import scala.collection.mutable

class ContigWindowIterator[I <: Interval](halfWindowSize: Int, regions: BufferedIterator[I])
  extends SkippableLociIterator[Iterable[I]] {

  private val queue = new mutable.PriorityQueue[I]()(Interval.orderByEnd[I])

  override def _advance: Option[(Locus, Iterable[I])] = {
    updateQueue()

    if (queue.isEmpty) {
      if (!regions.hasNext)
        return None

      locus = regions.head.start - halfWindowSize
      return _advance
    }

    Some(locus -> queue)
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
