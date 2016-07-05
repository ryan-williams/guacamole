package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.reference.{Contig, Region}

case class ContigIterator[+R <: Region](contig: Contig, regions: BufferedIterator[R])
  extends BufferedIterator[R] {

  override def head: R =
    if (hasNext)
      regions.head
    else
      throw new NoSuchElementException

  override def hasNext: Boolean = {
    regions.hasNext && regions.head.contig == contig
  }

  override def next(): R = {
    val n = regions.head
    regions.next()
    n
  }
}

object ContigIterator {
  def apply[R <: Region](regions: BufferedIterator[R]): ContigIterator[R] = {
    ContigIterator(regions.head.contig, regions)
  }
}

