/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bdgenomics.guacamole.windowing

import org.bdgenomics.guacamole.DistributedUtil.PerSample
import org.bdgenomics.guacamole.{ HasReferenceRegion, LociMap }

/**
 *
 * This iterator advances each per-sample SlidingWindow to the next locus.
 *
 * Each call to next advances each per sample SlidingWindow one locus (unless skipEmpty is set,
 * where it advances to the next locus where at least one window contains an element)
 *
 * @param ranges Collection of ranges the iterator should cover
 * @param skipEmpty Skip loci if the windows contain no overlapping regions
 * @param windows Sliding Window of regions
 */
case class LociInWindowsIterator[Region <: HasReferenceRegion](ranges: Iterator[LociMap.SimpleRange],
                                                               skipEmpty: Boolean,
                                                               windows: PerSample[SlidingWindow[Region]])
    extends Iterator[PerSample[SlidingWindow[Region]]] {

  private var currentRange: LociMap.SimpleRange = ranges.next()

  private var currentLocus: Long = -1L
  private var nextLocus: Option[Long] =
    if (skipEmpty)
      findNextNonEmptyLocus(currentRange.start)
    else
      Some(currentRange.start)

  // Check that there are regions in at least one window and the final base of the final element is before this locus
  private def currentElementsOverlapLocus(locus: Long): Boolean =
    windows.exists(_.endOfRange().exists(locus < _))

  // Check that the next element in any window overlaps [[locus]]
  private def nextElementOverlapsLocus(locus: Long): Boolean =
    windows.exists(_.nextElement.exists(_.overlapsLocus(locus)))

  /**
   * Whether there are any more loci left to process
   *
   * @return true if there are locus left to process and if skipEmpty is true, there is a locus with overlapping
   *         elements
   */
  override def hasNext: Boolean = {
    nextLocus.isDefined
  }

  /**
   * Next returns the sequences of windows we are iterating over, which have been each advanced to the next locus.
   * The window cover a region of (2 * halfWindowSize + 1) around the currentLocus
   * and contain all of the elements that overlap that region
   *
   * Each call to next() returns windows that been advanced to the next locus (and corresponding overlapping reads are
   * adjusted)
   *
   * @return The sliding windows advanced to the next locus (next nonEmpty locus if skipEmpty is true)
   */
  override def next(): PerSample[SlidingWindow[Region]] = {
    nextLocus match {
      case Some(locus) => {
        currentLocus = locus
        windows.foreach(_.setCurrentLocus(currentLocus))
        nextLocus = findNextLocus(currentLocus)
        windows
      }
      case None => throw new ArrayIndexOutOfBoundsException()
    }
  }

  /**
   * Identifies the next locus to process after [[locus]]
   *
   * @param locus Current locus that iterator is at
   * @return Next locus > [[locus]] to process, None if none exist
   */
  private def findNextLocus(locus: Long): Option[Long] = {
    var nextLocusInRangeOpt = nextLocusInRange(locus + 1)

    // If we are skipping empty loci and the the current window does not overlap the next locus
    if (skipEmpty && nextLocusInRangeOpt.exists(!currentElementsOverlapLocus(_))) {
      nextLocusInRangeOpt = findNextNonEmptyLocus(nextLocusInRangeOpt.get)
    }
    nextLocusInRangeOpt
  }

  /**
   * @param locus Find next non-empty locus >= [[locus]]
   * @return Next non-empty locus >= [[locus]], or None if none exist
   */
  private def findNextNonEmptyLocus(locus: Long): Option[Long] = {

    var nextLocusInRangeOpt: Option[Long] = Some(locus)
    // Find the next loci with overlapping elements that is in a valid range
    while (nextLocusInRangeOpt.exists(!nextElementOverlapsLocus(_))) {
      // Drop elements out of the window that are before the next locus in [[ranges]] and do not overlap
      windows.foreach(_.dropUntil(nextLocusInRangeOpt.get))

      // If any window has regions left find the minimum starting locus
      val nextLocusInWindows: Option[Long] = windows.flatMap(_.nextElement.map(_.start)).reduceOption(_ min _)

      // Find the next valid locus in ranges
      nextLocusInRangeOpt = nextLocusInWindows.flatMap(nextLocusInRange(_))
    }

    nextLocusInRangeOpt
  }

  /**
   *
   * Find the next locus is ranges that >= the given locus
   *
   * @param locus locus to find in ranges
   * @return The next locus >= [[locus]] which is contained in ranges
   */
  private def nextLocusInRange(locus: Long): Option[Long] = {
    if (currentRange.end > locus) {
      Some(locus)
    } else if (ranges.hasNext) {
      currentRange = ranges.next()
      Some(currentRange.start)
    } else {
      None
    }
  }
}
