package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.loci.set.LociIterator
import org.hammerlab.guacamole.reference.{ContigPosition, HasLocus}
import org.hammerlab.guacamole.util.OptionIterator

class FilterLociIterator[T <: HasLocus](allowedLoci: LociIterator, lociKeyedIter: SkippableLociIterator[T])
  extends OptionIterator[T] {

  override def _advance: Option[T] = {
    if (!allowedLoci.hasNext) return None
    if (!lociKeyedIter.hasNext) return None

    val ContigPosition(nextAllowedLocus) = allowedLoci.head
    val obj = lociKeyedIter.head
    val nextObjectLocus = obj.locus

    if (nextObjectLocus > nextAllowedLocus) {
      allowedLoci.skipTo(nextObjectLocus)
      _advance
    } else if (nextAllowedLocus > nextObjectLocus) {
      lociKeyedIter.skipTo(nextAllowedLocus)
      _advance
    } else {
      allowedLoci.next()
      lociKeyedIter.next()
      Some(obj)
    }
  }

}
