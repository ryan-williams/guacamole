package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.{ReferencePosition, ReferenceRegion}
import org.hammerlab.guacamole.util.OptionIterator


class WindowIterator[R <: ReferenceRegion](halfWindowSize: Int,
                                           loci: LociSet,
                                           regions: BufferedIterator[R])
  extends OptionIterator[(ReferencePosition, Iterable[R])] {

  var curContig: LociContigWindowIterator[R] = _
  var curContigName: String = _

  def clearContig(): Unit = {
    curContig = null
    while (regions.hasNext && regions.head.contig == curContigName) {
      regions.next()
    }
  }

  override def _advance: Option[(ReferencePosition, Iterable[R])] = {
    while (curContig == null) {

      if (!regions.hasNext)
        return None

      curContigName = regions.head.contig

      // Iterator over the loci on this contig allowed by input LociSet.
      val contigLoci = loci.onContig(curContigName).iterator

      // Iterator over regions on this contig.
      val contigRegions = ContigIterator(curContigName, regions)

      // Iterator over "piles" of regions (loci and the reads that overlap them, or a window around them) on this contig.
      val contigRegionWindows = new ContigWindowIterator(halfWindowSize, contigRegions)

      // Iterator that merges the loci allowed by the LociSet with the loci that have reads overlapping them.
      curContig = new LociContigWindowIterator(contigLoci, contigRegionWindows)

      if (!curContig.hasNext)
        clearContig()
    }

    val (locus, nextRegions) = curContig.next()

    Some(
      ReferencePosition(curContigName, locus) ->
        nextRegions
    )
  }

  override def postNext(n: (ReferencePosition, Iterable[R])): Unit = {
    if (!curContig.hasNext)
      clearContig()
  }

}
