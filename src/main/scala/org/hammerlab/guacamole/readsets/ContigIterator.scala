package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.reference.{Contig, ReferenceRegion}

case class ContigIterator[+R <: ReferenceRegion](contig: Contig, regions: BufferedIterator[R])
  extends Iterator[R] {

  override def hasNext: Boolean = {
    regions.hasNext && regions.head.contig == contig
  }

  override def next(): R = {
    if (hasNext)
      regions.next()
    else
      throw new NoSuchElementException
  }
}

object ContigIterator {
  def apply[R <: ReferenceRegion](regions: BufferedIterator[R]): ContigIterator[R] = {
    ContigIterator(regions.head.contig, regions)
  }
}

