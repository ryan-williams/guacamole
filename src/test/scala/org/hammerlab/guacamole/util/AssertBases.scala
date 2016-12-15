package org.hammerlab.guacamole.util

import org.hammerlab.genomics.bases.{ Base, Bases }
import org.scalatest.Matchers

object AssertBases extends Matchers {
  def apply(bases1: Bases, bases2: String) = bases1.toString === (bases2)
  def apply(bases1: Bases, base: Base) = bases1.toString === (Bases(base))
}
