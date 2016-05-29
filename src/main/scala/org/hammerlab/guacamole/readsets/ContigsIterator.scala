package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.reference.ReferenceRegion

class ContigsIterator[R <: ReferenceRegion](it: BufferedIterator[R])
  extends Iterator[ContigIterator[R]] {

  override def hasNext: Boolean = it.hasNext

  override def next(): ContigIterator[R] = ContigIterator(it)
}
