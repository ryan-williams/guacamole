package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.reference.{ReferencePosition, ReferenceRegion}

import scala.collection.mutable

class WindowDepthIterator[R <: ReferenceRegion] private(halfWindowSize: Int, regions: BufferedIterator[R])
  extends Iterator[(ReferencePosition, (Int, Int, Int))] {

  var pos: ReferencePosition = _

  val queue = mutable.PriorityQueue[ReferencePosition]()(ReferenceRegion.orderByEnd)

  override def hasNext: Boolean = regions.hasNext

  override def next(): (ReferencePosition, (Int, Int, Int)) = {
    pos = regions.head.startPos

    var numDropped = 0
    while (queue.headOption.exists(endPos => !(endPos > pos - halfWindowSize))) {
      queue.dequeue()
      numDropped += 1
    }

    var numAdded = 0
    while (regions.head.startPos <= pos + halfWindowSize) {
      queue.enqueue(regions.next().startPos)
      numAdded += 1
    }

    (pos, (queue.size, numDropped, numAdded))
  }
}

object WindowDepthIterator {
  def apply[R <: ReferenceRegion](halfWindowSize: Int, regions: Iterator[R]): WindowDepthIterator[R] =
    new WindowDepthIterator(halfWindowSize, regions.buffered)
}
