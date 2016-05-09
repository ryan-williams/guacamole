package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.loci.Coverage.PositionCoverage
import org.hammerlab.guacamole.reference.{Contig, ReferencePosition, ReferenceRegion}

import scala.collection.mutable

class LociCoverageIterator private(contig: Contig, regions: BufferedIterator[ReferenceRegion])
  extends Iterator[PositionCoverage] {

  private val ends = mutable.PriorityQueue[Long]()

  private var curPos = -1L
  private var _next: Coverage = _
  private var done = false

  private def advance(): Boolean = {
    if (regions.isEmpty && ends.isEmpty) return false

    if (curPos == -1L) {
      curPos = regions.head.start
    } else {
      curPos += 1
    }

    var numDropped = 0
    while (ends.headOption.exists(_ < curPos)) {
      ends.dequeue()
      numDropped += 1
    }

    var numAdded = 0
    while (regions.nonEmpty && regions.head.start == curPos) {
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

object LociCoverageIterator {
  def apply(regions: ContigIterator[ReferenceRegion]): LociCoverageIterator =
    new LociCoverageIterator(regions.contig, regions.buffered)
}
