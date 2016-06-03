package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.readsets.LociIntervalsPerSample.SampleInterval
import org.hammerlab.guacamole.reference.{HasLocus, Interval}
import org.hammerlab.guacamole.reference.ReferencePosition.Locus

import scala.collection.mutable

trait HasSampleId {
  def sampleId: Int
}

case class LociIntervalsPerSample[I <: SampleInterval](locus: Locus, intervals: PerSample[Iterable[I]]) extends HasLocus

class LociOverlapsPerSampleIterator[I <: SampleInterval](halfWindowSize: Int, numSamples: Int, regions: BufferedIterator[I])
  extends SkippableLociIterator[LociIntervalsPerSample[I]] {

  private val queues = Vector.fill(numSamples)(new mutable.PriorityQueue[I]()(Interval.orderByEnd[I]))

  override def _advance: Option[LociIntervalsPerSample[I]] = {
    updateQueue()

    if (queues.forall(_.isEmpty)) {
      if (!regions.hasNext)
        return None

      locus = regions.head.start - halfWindowSize
      return _advance
    }

    Some(LociIntervalsPerSample(locus, queues))
  }

  private def updateQueue(): Unit = {
    val lowerLimit = locus - halfWindowSize
    val upperLimit = locus + halfWindowSize

    for { queue <- queues } {
      while (queue.headOption.exists(_.end <= lowerLimit)) {
        queue.dequeue
      }
    }

    while (regions.hasNext && regions.head.start <= upperLimit) {
      if (regions.head.end > lowerLimit) {
        queues(regions.head.sampleId).enqueue(regions.head)
      }
      regions.next()
    }
  }

}

object LociIntervalsPerSample {
  type SampleInterval = Interval with HasSampleId
}
