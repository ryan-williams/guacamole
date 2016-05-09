package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.reference.{ReferencePosition, ReferenceRegion}

import scala.collection.mutable

class WindowIterator[R <: ReferenceRegion] private(halfWindowSize: Int, regions: BufferedIterator[R])
  extends Iterator[(ReferencePosition, (Iterable[R], Int, Int))] {

  var pos: ReferencePosition = _

  val queue = mutable.PriorityQueue[R]()(ReferenceRegion.orderByEnd)

  override def hasNext: Boolean = regions.hasNext

  override def next(): (ReferencePosition, (Iterable[R], Int, Int)) = {
    pos = regions.head.startPos

    var numDropped = 0
    while (queue.headOption.exists(r => !(r.endPos > pos - halfWindowSize))) {
      queue.dequeue()
      numDropped += 1
    }

    var numAdded = 0
    while (regions.head.startPos <= pos + halfWindowSize) {
      queue.enqueue(regions.next())
      numAdded += 1
    }

    (pos, (queue, numDropped, numAdded))
  }
}

object WindowIterator {
  def apply[R <: ReferenceRegion](halfWindowSize: Int, regions: Iterator[R]): WindowIterator[R] =
    new WindowIterator(halfWindowSize, regions.buffered)
}
