package org.hammerlab.guacamole.filters.pileup

import org.hammerlab.guacamole.pileup.Pileup.PileupElements

/**
 * Filter to remove pileup elements with low alignment quality
 */
object QualityAlignedReadsFilter {
  /**
   *
   * @param elements sequence of pileup elements to filter
   * @param minimumAlignmentQuality Threshold to define whether a read was poorly aligned
   * @return filtered sequence of elements - those who had higher than minimumAlignmentQuality alignmentQuality
   */
  def apply(elements: PileupElements, minimumAlignmentQuality: Int): PileupElements =
    elements.filter(_.read.alignmentQuality >= minimumAlignmentQuality)
}
