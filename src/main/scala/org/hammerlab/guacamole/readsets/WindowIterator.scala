package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.{ReferencePosition, ReferenceRegion}


class WindowIterator[R <: ReferenceRegion](halfWindowSize: Int,
                                           fromOpt: Option[ReferencePosition],
                                           untilOpt: Option[ReferencePosition],
                                           loci: LociSet,
                                           regions: BufferedIterator[R])
  extends Iterator[(ReferencePosition, Iterable[R])] {

  var curContig: ContigWindowIterator[R] = _
  var curContigName: String = _

  def advance(): Unit = {
    while ((curContig == null || !curContig.hasNext) && regions.hasNext) {
      if (curContig != null) {
        while (regions.hasNext && regions.head.contig == curContigName) {
          regions.next()
        }
      }
      curContigName = regions.head.contig
      val lociContig = loci.onContig(curContigName).iterator
      fromOpt.foreach(lociContig.skipTo)
      val boundedLoci = BoundedIterator(None, untilOpt, lociContig)
      if (boundedLoci.isEmpty) {
        while (regions.hasNext && regions.head.contig == curContigName) {
          regions.next()
        }
      } else {
        curContig =
          ContigWindowIterator(
            halfWindowSize,
            boundedLoci,
            ContigIterator(curContigName, regions)
          )
      }
    }
  }

  override def hasNext: Boolean = {
    advance()
    curContig != null && curContig.hasNext
  }

  override def next(): (ReferencePosition, Iterable[R]) = {
    advance()
    if (curContig == null) throw new NoSuchElementException
    curContig.next()
  }
}
