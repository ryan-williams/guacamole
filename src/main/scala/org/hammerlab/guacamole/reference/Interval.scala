package org.hammerlab.guacamole.reference

trait Interval {
  def start: Long
  def end: Long
}

object Interval {
  def orderByEnd[I <: Interval] = new Ordering[I] {
    override def compare(x: I, y: I): Int = x.end.compare(y.end)
  }
}
