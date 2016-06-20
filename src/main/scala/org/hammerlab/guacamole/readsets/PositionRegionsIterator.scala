package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.ReferencePosition.Locus
import org.hammerlab.guacamole.reference.{Contig, ContigPosition, HasLocus, ReferencePosition, ReferenceRegion}
import org.hammerlab.magic.iterator.OptionIterator

//class PositionRegions[R <: ReferenceRegion](pos: ReferencePosition,
//                                            regions: Iterable[R])
//  extends Tuple2[ReferencePosition, Iterable[R]](pos, regions) {
//
//  def contig = pos.contig
//  def locus = pos.locus
//}

abstract class PositionRegionsIteratorBase[R <: ReferenceRegion, T <: HasLocus, U](halfWindowSize: Int,
                                                                                   loci: LociSet,
                                                                                   forceCallLoci: LociSet,
                                                                                   regions: BufferedIterator[R],
                                                                                   empty: Locus => T)
  extends OptionIterator[(ReferencePosition, U)] {

  def newObjIterator(contigRegions: ContigIterator[R]): SkippableLociIterator[T]

  def objToResult(t: T): U

  var curContigLociIterator: Iterator[T] = _
  var curContig: Contig = _

  def clearContig(): Unit = {
    curContigLociIterator = null
    while (regions.hasNext && regions.head.contig == curContig) {
      regions.next()
    }
  }

  override def _advance: Option[(ReferencePosition, U)] = {
    while (curContigLociIterator == null) {

      if (!regions.hasNext)
        return None

      // We will restrict ourselves to loci and regions on this contig in this iteration of the loop.
      curContig = regions.head.contig

      // Iterator over the loci on this contig allowed by the input LociSet.
      val contigLoci = loci.onContig(curContig).iterator

      // Positions on this contig that we must emit records at, even if the underlying data would otherwise skip them.
      val forceCallContigLoci = forceCallLoci.onContig(curContig).iterator

      // Restrict to regions on the current contig.
      val contigRegions = ContigIterator(curContig, regions)

      // Iterator over "piles" of regions (loci and the reads that overlap them, or a window around them) on this contig.
      val contigRegionObjs = newObjIterator(contigRegions)

      // Iterator that merges the loci allowed by the LociSet with the loci that have reads overlapping them.
      curContigLociIterator = new UnionLociIterator(
        forceCallContigLoci.map(pos => empty(pos)).buffered,
        new IntersectLociIterator(contigLoci, contigRegionObjs)
      )

      if (!curContigLociIterator.hasNext)
        clearContig()
    }

    // We can only get here when curContig != null && curContig.hasNext. We return None in the loop if that can no
    // longer happen, which signals that this iterator is done.

    val obj = curContigLociIterator.next()

    Some(
      ReferencePosition(curContig, obj.locus) -> objToResult(obj)
    )
  }

  override def postNext(): Unit = {
    if (!curContigLociIterator.hasNext)
      clearContig()
  }

}

class PositionRegionsIterator[R <: ReferenceRegion](halfWindowSize: Int,
                                                    loci: LociSet,
                                                    forceCallLoci: LociSet,
                                                    regions: BufferedIterator[R])
  extends PositionRegionsIteratorBase[R, LociIntervals[R], Iterable[R]](
    halfWindowSize,
    loci,
    forceCallLoci,
    regions,
    LociIntervals(_, Nil)
  ) {

  override def newObjIterator(contigRegions: ContigIterator[R]): SkippableLociIterator[LociIntervals[R]] =
    new LociOverlapsIterator(halfWindowSize, contigRegions)

  override def objToResult(t: LociIntervals[R]): Iterable[R] = t.intervals
}

class PositionRegionsPerSampleIterator[R <: ReferenceRegion with HasSampleId](halfWindowSize: Int,
                                                                              numSamples: Int,
                                                                              loci: LociSet,
                                                                              forceCallLoci: LociSet,
                                                                              regions: BufferedIterator[R])
  extends PositionRegionsIteratorBase[R, LociIntervalsPerSample[R], PerSample[Iterable[R]]](
    halfWindowSize,
    loci,
    forceCallLoci,
    regions,
    LociIntervalsPerSample(_, Vector.fill(numSamples)(Nil))
  ) {

  override def newObjIterator(contigRegions: ContigIterator[R]): SkippableLociIterator[LociIntervalsPerSample[R]] =
    new LociOverlapsPerSampleIterator(halfWindowSize, numSamples, contigRegions)

  override def objToResult(t: LociIntervalsPerSample[R]): PerSample[Iterable[R]] = t.intervals
}
