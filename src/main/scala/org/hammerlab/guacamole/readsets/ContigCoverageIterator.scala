package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.loci.Coverage.PositionCoverage
import org.hammerlab.guacamole.loci.set.{LociIterator, ContigIterator => LociContigIterator}
import org.hammerlab.guacamole.reference.{Contig, Interval, ReferencePosition, ReferenceRegion}

import scala.collection.mutable

class ContigCoverageIterator private(halfWindowSize: Int,
                                     contigLength: Long,
                                     contig: Contig,
                                     regions: BufferedIterator[Interval],
                                     loci: LociIterator)
  extends Iterator[PositionCoverage] {

  private val ends = mutable.PriorityQueue[Long]()(implicitly[Ordering[Long]].reverse)

  private var curPos = -1L
  private var _next: Coverage = _

  private def advance(): Boolean = {
    if (!loci.hasNext)
      return false
    else {
      val nextLocus = loci.head
      if (ends.isEmpty) {
        if (regions.isEmpty)
          return false

        val nextReadWindowStart = regions.head.start - halfWindowSize
        if (nextReadWindowStart > nextLocus) {
          curPos = nextReadWindowStart
          loci.skipTo(nextReadWindowStart)
        }
      }
      curPos = loci.next()
    }

    val lowerLimit = math.max(0, curPos - halfWindowSize)
    val upperLimit = math.min(contigLength, curPos + halfWindowSize)

    var numDropped = 0
    while (ends.headOption.exists(_ <= lowerLimit)) {
      ends.dequeue()
      numDropped += 1
    }

    if (curPos == contigLength) {
      numDropped += ends.dequeueAll.size
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

object ContigCoverageIterator {
  def apply(halfWindowSize: Int,
            contigLength: Long,
            regions: ContigIterator[ReferenceRegion],
            loci: LociIterator): ContigCoverageIterator =
    new ContigCoverageIterator(halfWindowSize, contigLength, regions.contig, regions.buffered, loci)
}