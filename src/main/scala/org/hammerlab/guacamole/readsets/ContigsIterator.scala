package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.reference.ReferenceRegion

class ContigsIterator[R <: ReferenceRegion] private(it: BufferedIterator[R])
  extends Iterator[ContigIterator[R]] {

  override def hasNext: Boolean = it.hasNext

  override def next(): ContigIterator[R] = {
    ContigIterator(it)
  }
}

object ContigsIterator {
  def apply[R <: ReferenceRegion](it: Iterator[R]): ContigsIterator[R] = new ContigsIterator(it.buffered)
}
