package org.hammerlab.guacamole.loci

import org.hammerlab.guacamole.reference.Position

case class WindowCoverage(depth: Int = 0,
                          nextStarts: Int = 0,
                          nextEnds: Int = 0,
                          prevStarts: Int = 0,
                          prevEnds: Int = 0) {
  def +(o: WindowCoverage): WindowCoverage =
    WindowCoverage(
      depth + o.depth,
      nextStarts + o.nextStarts,
      nextEnds + o.nextEnds,
      prevStarts + o.prevStarts,
      prevEnds + o.prevEnds
    )

  def nextGain: Int = nextStarts - nextEnds
  def prevGain: Int = prevEnds - prevStarts
  def nextLoss: Int = -nextGain
  def prevLoss: Int = -prevGain

  override def toString: String = s"$depth,$nextStarts,$nextEnds,$prevStarts,$prevEnds)"
}

case class Coverage(depth: Int = 0, starts: Int = 0, ends: Int = 0) {
  def +(o: Coverage): Coverage =
    Coverage(
      depth + o.depth,
      starts + o.starts,
      ends + o.ends
    )

  override def toString: String = s"($depth,$starts,$ends)"
}

object Coverage {
  type PositionCoverage = (Position, Coverage)
}
