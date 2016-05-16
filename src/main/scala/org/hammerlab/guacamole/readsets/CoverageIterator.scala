package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.loci.Coverage.PositionCoverage
import org.hammerlab.guacamole.reference.{Contig, ReferencePosition, ReferenceRegion}

import scala.collection.mutable

class CoverageIterator private(halfWindowSize: Int,
                               contig: Contig,
                               regions: BufferedIterator[ReferenceRegion])
  extends Iterator[PositionCoverage] {

  private val ends = mutable.PriorityQueue[Long]()(implicitly[Ordering[Long]].reverse)

  private var curPos = -1L
  private var _next: Coverage = _
  private var done = false

  private def advance(): Boolean = {
    if (ends.isEmpty) {
      if (regions.isEmpty)
        return false

      curPos = regions.head.start - halfWindowSize
    } else {
      curPos += 1
    }

    val lowerLimit = curPos - halfWindowSize
    val upperLimit = curPos + halfWindowSize

    var numDropped = 0
    while (ends.headOption.exists(_ <= lowerLimit)) {
      ends.dequeue()
      numDropped += 1
    }

    var numAdded = 0
    while (regions.nonEmpty && regions.head.start <= upperLimit) {
      ends.enqueue(regions.next().end)
      numAdded += 1
    }

    _next = Coverage(ends.size, numAdded, numDropped)
    true
  }

  override def hasNext: Boolean = {
    _next != null || advance()
  }

  override def next(): PositionCoverage = {
    val r =
      if (_next == null) {
        if (!advance()) throw new NoSuchElementException
        _next
      } else
        _next

    _next = null

    ReferencePosition(contig, curPos) -> r
  }
}

object CoverageIterator {
  def apply(halfWindowSize: Int, regions: ContigIterator[ReferenceRegion]): CoverageIterator =
    new CoverageIterator(halfWindowSize, regions.contig, regions.buffered)
}
