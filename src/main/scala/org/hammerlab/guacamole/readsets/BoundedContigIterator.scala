package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.reference.{Contig, ReferencePosition, ReferenceRegion}

class BoundedContigIterator[R <: ReferenceRegion] private(stopAt: Long, contigRegionsRaw: ContigIterator[R])
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
  def apply[R <: ReferenceRegion](contig: Contig, stopAt: Long, regions: Iterator[R]): BoundedContigIterator[R] =
    new BoundedContigIterator(stopAt, new ContigIterator(contig, regions.buffered))

  def apply[R <: ReferenceRegion](lociToTake: Long, regions: Iterator[R]): Iterator[R] = {
    if (regions.hasNext) {
      val buffered = regions.buffered
      val ReferencePosition(contig, start) = buffered.head.startPos
      BoundedContigIterator(contig, start + lociToTake, buffered)
    } else
      Iterator()
  }
}
