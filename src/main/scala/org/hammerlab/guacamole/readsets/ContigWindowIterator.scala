package org.hammerlab.guacamole.readsets

import java.util.NoSuchElementException

import org.hammerlab.guacamole.loci.set.LociIterator
import org.hammerlab.guacamole.reference.ReferencePosition.Locus
import org.hammerlab.guacamole.reference.{Contig, Interval, ReferencePosition, ReferenceRegion}

import scala.collection.mutable

class ContigWindowIterator[I <: Interval] private(contig: Contig,
                                                  halfWindowSize: Int,
                                                  //loci: LociIterator,
                                                  regions: BufferedIterator[I])
  extends Iterator[(ReferencePosition, Iterable[I])] {

  private val queue = new mutable.PriorityQueue[I]()(Interval.orderByEnd[I])

  private var _next: (ReferencePosition, Iterable[I]) = _

  def advance(): Boolean = {
    if (_next != null) return true

    if (skipEmpty) {
      var advancing = true
      while (advancing) {
        advancing = false

        if (!loci.hasNext) return false

        if (regions.hasNext && loci.head < regions.head.start - halfWindowSize) {
          loci.skipTo(regions.head.start - halfWindowSize)
          if (!loci.hasNext) return false
          advancing = true
        }

        while (regions.hasNext && regions.head.end + halfWindowSize <= loci.head) {
          regions.next()
        }
      }
    } else
      if (!loci.hasNext)
        return false

    val locus = loci.next()

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

    if (skipEmpty && queue.isEmpty) return false

    _next = ReferencePosition(contig, locus) -> queue.view

    true
  }

  override def hasNext: Boolean = advance()

  override def next(): (ReferencePosition, Iterable[I]) = {
    if (!advance()) throw new NoSuchElementException
    val n = _next
    _next = null
    n
//    val pos = loci.next()
//
//    val lowerLimit = pos - halfWindowSize
//    val upperLimit = pos + halfWindowSize
//
//    while (ends.headOption.exists(e => !(e > lowerLimit))) {
//      ends.dequeue
//    }
//
//    while (queue.headOption.exists(r => !(r.end > lowerLimit))) {
//      queue.dequeue
//    }
//
//    while (regions.hasNext && regions.head.start <= upperLimit) {
//      if (regions.head.end > lowerLimit) {
//        ends.enqueue(regions.head.end)
//        queue.enqueue(regions.head)
//      }
//      regions.next()
//    }
//
//    ReferencePosition(contig, pos) ->
//      queue.view.filter(_.end > lowerLimit)
  }
}

object ContigWindowIterator {
  def apply[I <: Interval](contig: Contig,
                           halfWindowSize: Int,
                           loci: LociIterator,
                           regions: Iterator[I],
                           skipEmpty: Boolean = true): ContigWindowIterator[I] =
    new ContigWindowIterator(contig, halfWindowSize, loci, regions.buffered, skipEmpty)
}
