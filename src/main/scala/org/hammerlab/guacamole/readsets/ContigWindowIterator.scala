package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.reference.{ReferencePosition, ReferenceRegion}

import scala.collection.mutable

class ContigWindowIterator[R <: ReferenceRegion] private(halfWindowSize: Int,
                                                         loci: Iterator[ReferencePosition],
                                                         regions: BufferedIterator[R])
  extends Iterator[(ReferencePosition, Iterable[R])] {

  val queue = mutable.Queue[R]()
  val ends = mutable.PriorityQueue[ReferencePosition]()(ReferenceRegion.orderByEnd)

  override def hasNext: Boolean = loci.hasNext

  override def next(): (ReferencePosition, Iterable[R]) = {

    val pos = loci.next()

    val lowerLimit = pos - halfWindowSize
    val upperLimit = pos + halfWindowSize

    while (ends.headOption.exists(e => !(e > lowerLimit))) {
      ends.dequeue
    }

    while (queue.headOption.exists(r => !(r.endPos > lowerLimit))) {
      queue.dequeue
    }

    while (regions.hasNext && regions.head.startPos <= upperLimit) {
      if (regions.head.endPos > lowerLimit) {
        ends.enqueue(regions.head.endPos)
        queue.enqueue(regions.head)
      }
      regions.next()
    }

    (pos, queue.view.filter(_.endPos > lowerLimit))
  }
}

object ContigWindowIterator {
  def apply[R <: ReferenceRegion](halfWindowSize: Int,
                                  loci: Iterator[ReferencePosition],
                                  regions: Iterator[R]): ContigWindowIterator[R] =
    new ContigWindowIterator(halfWindowSize, loci, regions.buffered)
}
