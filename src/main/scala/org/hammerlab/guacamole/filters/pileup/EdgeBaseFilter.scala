package org.hammerlab.guacamole.filters.pileup

import org.hammerlab.guacamole.pileup.Pileup.PileupElements

/**
 * Filter to remove pileup elements close to edge of reads
 */
object EdgeBaseFilter {
  /**
   *
   * @param elements sequence of pileup elements to filter
   * @param minimumDistanceFromEndFromRead Threshold of distance from base to edge of read
   * @return filtered sequence of elements - those who were further from directional end minimumDistanceFromEndFromRead
   */
  def apply(elements: PileupElements, minimumDistanceFromEndFromRead: Int): PileupElements =
    elements.filter(_.distanceFromSequencingEnd >= minimumDistanceFromEndFromRead)
}
