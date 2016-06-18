package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.reference.{ContigPosition, HasLocus}
import org.hammerlab.magic.iterator.OptionIterator

/**
 * Merge two iterators of objects that exist at discrete genomic positions on one (shared) contig.
 *
 * Optionally
 *
 * @param loci
 * @param lociObjs
 * @tparam T
 */
class IntersectLociIterator[+T <: HasLocus, +U <: HasLocus](loci: SkippableLociIterator[T],
                                                            lociObjs: SkippableLociIterator[U])
  extends OptionIterator[U] {

  override def _advance: Option[U] = {
    if (!loci.hasNext) return None
    if (!lociObjs.hasNext) return None

    val ContigPosition(nextAllowedLocus) = loci.head
    val obj = lociObjs.head
    val nextObjectLocus = obj.locus

    if (nextObjectLocus > nextAllowedLocus) {
      loci.skipTo(nextObjectLocus)
      _advance
    } else if (nextAllowedLocus > nextObjectLocus) {
      lociObjs.skipTo(nextAllowedLocus)
      _advance
    } else {
      loci.next()
      lociObjs.next()
      Some(obj)
    }
  }

}
