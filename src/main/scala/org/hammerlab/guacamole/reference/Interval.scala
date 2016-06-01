package org.hammerlab.guacamole.reference

import org.hammerlab.guacamole.reference.ReferencePosition.Locus

trait Interval {
  def start: Locus
  def end: Locus

  def contains(locus: Locus): Boolean = start <= locus && locus < end
}

object Interval {
  def orderByEnd[I <: Interval] = new Ordering[I] {
    override def compare(x: I, y: I): Int = x.end.compare(y.end)
  }
}
