package org.hammerlab.guacamole.readsets

import java.util.NoSuchElementException

import org.hammerlab.guacamole.reference.ReferencePosition.Locus
import org.hammerlab.guacamole.reference.{Contig, Interval, ReferencePosition}

import scala.collection.mutable

class ContigWindowIterator[I <: Interval](contig: Contig,
                                          halfWindowSize: Int,
                                          regions: BufferedIterator[I])
  extends BufferedIterator[(ReferencePosition, Iterable[I])] {

  private val queue = new mutable.PriorityQueue[I]()(Interval.orderByEnd[I])

  var locus: Locus = 0
  private var _next: (ReferencePosition, Iterable[I]) = _

  override def head: (ReferencePosition, Iterable[I]) = {
    if (!hasNext) throw new NoSuchElementException
    _next
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

  override def hasNext: Boolean = {
    if (_next != null) return true

    updateQueue()

    if (queue.isEmpty) {
      if (!regions.hasNext)
        return false

      locus = regions.head.start - halfWindowSize
      return hasNext
    }

    _next = ReferencePosition(contig, locus) -> queue

    true
  }

  override def next(): (ReferencePosition, Iterable[I]) = {
    if (!hasNext) throw new NoSuchElementException
    val n = _next
    _next = null
    locus += 1
    n
  }

  def skipTo(newLocus: Locus): this.type = {
    if (newLocus > locus) {
      locus = newLocus
      //updateQueue()
      _next = null
//      if (queue.isEmpty) {
//        locus = -1L
//      }
    }
    this
  }
}
