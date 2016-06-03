package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.{ReferencePosition, ReferenceRegion}


class WindowIterator[R <: ReferenceRegion](halfWindowSize: Int,
//                                           fromOpt: Option[ReferencePosition],
//                                           untilOpt: Option[ReferencePosition],
                                           loci: LociSet,
                                           regions: BufferedIterator[R])
  extends Iterator[(ReferencePosition, Iterable[R])] {

  var curContig: LociContigWindowIterator[R] = _
  var curContigName: String = _

  def advance(): Boolean = {
    if (curContig != null && !curContig.hasNext) curContig = null

    while (curContig == null) {
      if (curContigName != null) {
        while (regions.hasNext && regions.head.contig == curContigName) {
          regions.next()
        }
      }

      if (!regions.hasNext)
        return false

      curContigName = regions.head.contig
      val contigLoci = loci.onContig(curContigName).iterator

//      for {
//        ReferencePosition(contig, fromLocus) <- fromOpt
//        if contig == curContigName
//      } {
//        contigLoci.skipTo(fromLocus)
//      }
//
//      for {
//        ReferencePosition(contig, untilLocus) <- untilOpt
//        if contig == curContigName
//      } {
//        contigLoci.stopAt(untilLocus)
//      }

      curContig =
        new LociContigWindowIterator(
          contigLoci,
          new ContigWindowIterator(
            curContigName,
            halfWindowSize,
            ContigIterator(curContigName, regions)
          )
        )

      if (!curContig.hasNext)
        curContig = null
    }

    true
  }

  override def hasNext: Boolean = advance()

  override def next(): (ReferencePosition, Iterable[R]) = {
    if (!advance()) throw new NoSuchElementException
    curContig.next()
  }
}
