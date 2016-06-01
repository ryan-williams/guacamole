package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.{ReferencePosition, ReferenceRegion}


class WindowIterator[R <: ReferenceRegion](halfWindowSize: Int,
                                           fromOpt: Option[ReferencePosition],
                                           untilOpt: Option[ReferencePosition],
                                           loci: LociSet,
                                           regions: BufferedIterator[R])
  extends Iterator[(ReferencePosition, Iterable[R])] {
  println(s"window has regions: $regions.hasNext")

  var curContig: ContigWindowIterator[R] = _
  var curContigName: String = _

  println(s"initialized contig name: $curContigName")

  def advance(): Boolean = {
    if (curContig != null && !curContig.hasNext) curContig = null

    while (curContig == null) {
      if (curContigName != null) {
        println(s"Burning reads from contig $curContigName")
        while (regions.hasNext && regions.head.contig == curContigName) {
          val r = regions.next()
          println(s"burnt: ${r.start}")
        }
      }
      if (!regions.hasNext) {
        println(s"out of reads on $curContigName")
        return false
      }

      curContigName = regions.head.contig
      println(s"set contig name: $curContigName")
      val contigLoci = loci.onContig(curContigName).iterator

      for {
        ReferencePosition(contig, fromLocus) <- fromOpt
        if contig == contig
      } {
        contigLoci.skipTo(fromLocus)
      }

      for {
        ReferencePosition(contig, untilLocus) <- untilOpt
        if contig == contig
      } {
        contigLoci.stopAt(untilLocus)
      }

      println(s"loci on contig $curContigName ($fromOpt, $untilOpt): ${contigLoci.hasNext}")

      curContig =
        ContigWindowIterator(
          curContigName,
          halfWindowSize,
          contigLoci,
          ContigIterator(curContigName, regions)
        )

      if (!curContig.hasNext) {
        curContig = null
        println(s"contig $curContigName found to be empty")
      }
    }

    true
  }

  override def hasNext: Boolean = advance()

  override def next(): (ReferencePosition, Iterable[R]) = {
    if (!advance()) throw new NoSuchElementException
    val r = curContig.next()
    println(s"from: $fromOpt: ${r._1} ${r._2.size}")
    r
  }
}
