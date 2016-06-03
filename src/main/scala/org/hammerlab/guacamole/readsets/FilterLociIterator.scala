package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.loci.set.LociIterator
import org.hammerlab.guacamole.reference.ReferencePosition.Locus
import org.hammerlab.guacamole.reference.{Contig, Interval, ReferencePosition}
import org.hammerlab.guacamole.util.OptionIterator

class LociContigWindowIterator[I <: Interval](loci: LociIterator, regionsIter: ContigWindowIterator[I])
  extends OptionIterator[(Locus, Iterable[I])] {

  override def _advance: Option[(Locus, Iterable[I])] = {
    if (!loci.hasNext) return None
    if (!regionsIter.hasNext) return None

    val nextLocus = loci.head
    val (nextRegionsLocus, regions) = regionsIter.head

    if (nextRegionsLocus > nextLocus) {
      loci.skipTo(nextRegionsLocus)
      _advance
    } else if (nextLocus > nextRegionsLocus) {
      regionsIter.skipTo(nextLocus)
      _advance
    } else {
      loci.next()
      regionsIter.next()
      Some(nextRegionsLocus -> regions)
    }
  }

}
