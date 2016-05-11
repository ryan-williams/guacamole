package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.{ReferencePosition, ReferenceRegion}

import scala.collection.immutable.Queue
import scala.collection.mutable

class WindowIterator[R <: ReferenceRegion] private(halfWindowSize: Int,
                                                   loci: Iterator[ReferencePosition],
                                                   regions: BufferedIterator[R])
  extends Iterator[(ReferencePosition, (Iterable[R], Int, Int))] {

  val queue = mutable.Queue[R]()
  val ends = mutable.PriorityQueue[ReferencePosition]()(ReferenceRegion.orderByEnd)

  override def hasNext: Boolean = loci.hasNext

  override def next(): (ReferencePosition, (Iterable[R], Int, Int)) = {
    val pos = loci.next()

    val lowerLimit = pos - halfWindowSize
    val upperLimit = pos + halfWindowSize

    var numDropped = 0
    while (ends.headOption.exists(e => !(e > lowerLimit))) {
      ends.dequeue
      numDropped += 1
    }

    while (queue.headOption.exists(r => !(r.endPos > lowerLimit))) {
      queue.dequeue
    }

    var numAdded = 0
    while (regions.hasNext && regions.head.startPos <= upperLimit) {
      ends.enqueue(regions.head.endPos)
      queue.enqueue(regions.next())
      numAdded += 1
    }

    (pos, (queue.view.filter(_.endPos > lowerLimit), numDropped, numAdded))
  }
}

object WindowIterator {
  def apply[R <: ReferenceRegion](halfWindowSize: Int,
                                  loci: LociSet,
                                  regions: Iterator[R]): WindowIterator[R] =
    WindowIterator(halfWindowSize, loci.iterator, regions)

  def apply[R <: ReferenceRegion](halfWindowSize: Int,
                                  loci: Iterator[ReferencePosition],
                                  regions: Iterator[R]): WindowIterator[R] =
    new WindowIterator(halfWindowSize, loci, regions.buffered)
}
