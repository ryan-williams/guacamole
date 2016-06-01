package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.loci.set.{LociIterator, LociSet, ContigIterator => LociContigIterator}
import org.hammerlab.guacamole.reference.ReferenceRegion

class ContigsIterator[R <: ReferenceRegion](it: BufferedIterator[R], loci: LociSet)
  extends Iterator[(ContigIterator[R], LociIterator)] {

  override def hasNext: Boolean = it.hasNext

  override def next(): (ContigIterator[R], LociIterator) = {
    val contigRegions = ContigIterator(it)
    val lociContig = loci.onContig(contigRegions.contig).iterator
    (contigRegions, lociContig)
  }
}
