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

package org.hammerlab.guacamole.reference

/**
 * Trait for objects that are associated with an interval on the genome. The most prominent example is a
 * [[org.hammerlab.guacamole.reads.MappedRead]], but there's also [[org.hammerlab.guacamole.variants.ReferenceVariant]].
 */
trait ReferenceRegion extends Interval {

  /** Name of the reference contig */
  def contig: Contig

  /** Start position on the genome, inclusive. Must be non-negative. */
  def start: Long
  def startPos = ReferencePosition(contig, start)

  /** The end position on the genome, *exclusive*. Must be non-negative. */
  def end: Long
  def endPos = ReferencePosition(contig, end)

  /**
   * Does the region overlap the given locus, with halfWindowSize padding?
   */
  def overlapsLocus(locus: Long, halfWindowSize: Long = 0): Boolean = {
    start - halfWindowSize <= locus && end + halfWindowSize > locus
  }

  /**
   * Does the region overlap another reference region
   *
   * @param other another region on the genome
   * @return True if the the regions overlap
   */
  def overlaps(other: ReferenceRegion): Boolean = {
    other.contig == contig && (overlapsLocus(other.start) || other.overlapsLocus(start))
  }

  def regionStr: String = s"${contig}:[$start-$end)"
}

object ReferenceRegion {
  // Order regions by start locus, increasing.
  def orderByStart[R <: ReferenceRegion] =
    new Ordering[R] {
      def compare(first: R, second: R) = second.start.compare(first.start)
    }

  // Order regions by end locus, increasing.
  def orderByEnd[R <: ReferenceRegion] =
    new Ordering[R] {
      def compare(first: R, second: R) = second.end.compare(first.end)
    }

  implicit def intraContigPartialOrdering[R <: ReferenceRegion] =
    new PartialOrdering[R] {
      override def tryCompare(x: R, y: R): Option[Int] = {
        if (x.contig == y.contig)
          Some(x.start.compare(y.start))
        else
          None
      }

      override def lteq(x: R, y: R): Boolean = {
        x.contig == y.contig && x.start <= y.start
      }
    }

  val contigMap: Map[Contig, Int] =
    Map(
      "1" -> 1,
      "2" -> 2,
      "3" -> 3,
      "4" -> 4,
      "5" -> 5,
      "6" -> 6,
      "7" -> 7,
      "8" -> 8,
      "9" -> 9,
      "10" -> 10,
      "11" -> 11,
      "12" -> 12,
      "13" -> 13,
      "14" -> 14,
      "15" -> 15,
      "16" -> 16,
      "17" -> 17,
      "18" -> 18,
      "19" -> 19,
      "20" -> 20,
      "21" -> 21,
      "22" -> 22,
      "X" -> 23,
      "Y" -> 24
    )

  val notFound = contigMap.size + 1

  def getContigRank(contig: Contig): Int = contigMap.getOrElse(normalizeContig(contig), notFound)
  def normalizeContig(contig: Contig): Contig = if (contig.startsWith("chr")) contig.drop(3) else contig

  val contigOrdering =
    new Ordering[Contig] {
      override def compare(x: Contig, y: Contig): Int = {
        getContigRank(x) - getContigRank(y)
      }
    }
}

case class Region(contig: String, start: Long, end: Long) extends ReferenceRegion
