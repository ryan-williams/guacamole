package org.hammerlab.guacamole.readsets.iterator.overlaps

import org.hammerlab.guacamole.loci.iterator.SkippableLocusKeyedIterator
import org.hammerlab.guacamole.reads.HasSampleId
import org.hammerlab.guacamole.readsets.iterator.overlaps.LociOverlapsPerSampleIterator.SampleInterval
import org.hammerlab.guacamole.readsets.{NumSamples, PerSample}
import org.hammerlab.guacamole.reference.{Interval, Locus}

import scala.collection.mutable

/**
 *  For each locus overlapped by @regions (± @halfWindowSize), emit the locus and the regions that overlap it (grouped
 *  by sample ID).
 */
case class LociOverlapsPerSampleIterator[I <: SampleInterval](numSamples: NumSamples,
                                                              halfWindowSize: Int,
                                                              sampleIntervals: BufferedIterator[I])
  extends SkippableLocusKeyedIterator[PerSample[Iterable[I]]] {

  private val queues =
    Vector.fill(
      numSamples
    )(
      new mutable.PriorityQueue[I]()(Interval.orderByEndDesc[I])
    )

  override def _advance: Option[(Locus, PerSample[Iterable[I]])] = {
    updateQueue()

    if (queues.forall(_.isEmpty)) {
      if (!sampleIntervals.hasNext)
        return None

      locus = sampleIntervals.head.start - halfWindowSize
      return _advance
    }

    Some(locus -> queues)
  }

  private def updateQueue(): Unit = {
    val lowerLimit = locus - halfWindowSize
    val upperLimit = locus + halfWindowSize

    for { queue <- queues } {
      while (queue.headOption.exists(_.end <= lowerLimit)) {
        queue.dequeue
      }
    }

    while (sampleIntervals.hasNext && sampleIntervals.head.start <= upperLimit) {
      if (sampleIntervals.head.end > lowerLimit) {
        queues(sampleIntervals.head.sampleId).enqueue(sampleIntervals.head)
      }
      sampleIntervals.next()
    }
  }
}

object LociOverlapsPerSampleIterator {
  type SampleInterval = Interval with HasSampleId
}
