package org.hammerlab.guacamole.reference

import org.hammerlab.guacamole.reference.ReferencePosition.Locus

case class ContigPosition(locus: Locus) extends HasLocus

object ContigPosition {
  implicit def contigPositionToLocus(contigPosition: ContigPosition): Locus = contigPosition.locus
}
