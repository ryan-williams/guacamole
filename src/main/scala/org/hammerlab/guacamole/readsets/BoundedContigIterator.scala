package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.reference.{Contig, Position, Region}

class BoundedContigIterator[R <: Region] private(stopAt: Long, contigRegionsRaw: ContigIterator[R])
  extends Iterator[R] {

  val contigRegions = contigRegionsRaw.buffered

  override def hasNext: Boolean = {
    contigRegions.hasNext && contigRegions.head.start < stopAt
  }

  override def next(): R = {
    if (hasNext)
      contigRegions.next()
    else
      throw new NoSuchElementException
  }
}

object BoundedContigIterator {
  def apply[R <: Region](contig: Contig, stopAt: Long, regions: Iterator[R]): BoundedContigIterator[R] =
    new BoundedContigIterator(stopAt, new ContigIterator(contig, regions.buffered))

  def apply[R <: Region](lociToTake: Long, regions: Iterator[R]): Iterator[R] = {
    if (regions.hasNext) {
      val buffered = regions.buffered
      val Position(contig, end) = buffered.head.endPos
      BoundedContigIterator(contig, end + lociToTake, buffered)
    } else
      Iterator()
  }
}
