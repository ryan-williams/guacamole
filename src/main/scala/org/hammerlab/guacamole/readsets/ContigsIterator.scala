package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.loci.set.{LociIterator, LociSet}
import org.hammerlab.guacamole.reference.ReferenceRegion
import org.hammerlab.guacamole.util.OptionIterator

class ContigsIterator[R <: ReferenceRegion](it: BufferedIterator[R], loci: LociSet)
  extends OptionIterator[(ContigIterator[R], LociIterator)] {

  var contigRegions: ContigIterator[R] = _

  override def _advance: Option[(ContigIterator[R], LociIterator)] = {
    if (contigRegions != null) {
      while (contigRegions.hasNext) {
        contigRegions.next()
      }
    }
    contigRegions = null

    if (!it.hasNext)
      None
    else {
      contigRegions = ContigIterator(it)
      val lociContig = loci.onContig(contigRegions.contig).iterator
      Some((contigRegions, lociContig))
    }
  }
}
