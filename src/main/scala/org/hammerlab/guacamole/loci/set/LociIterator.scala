package org.hammerlab.guacamole.loci.set

import org.hammerlab.guacamole.readsets.SkippableLociIterator
import org.hammerlab.guacamole.reference.{ContigPosition, HasLocus, Interval}

class LociIterator(intervals: BufferedIterator[Interval]) extends SkippableLociIterator[ContigPosition] {

  override def _advance: Option[ContigPosition] = {
    if (!intervals.hasNext)
      None
    else if (intervals.head.contains(locus))
      Some(ContigPosition(locus))
    else if (intervals.head.end <= locus) {
      intervals.next()
      _advance
    } else {
      locus = intervals.head.start
      Some(ContigPosition(locus))
    }
  }
}
