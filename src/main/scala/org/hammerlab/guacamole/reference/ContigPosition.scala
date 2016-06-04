package org.hammerlab.guacamole.reference

import org.hammerlab.guacamole.reference.ReferencePosition.Locus

case class ContigPosition(locus: Locus) extends HasLocus {
  override def equals(o: Any): Boolean =
    o match {
      case ContigPosition(l) => locus == l
      case l: Locus => locus == l
      case i: Int => locus == i
      case _ => false
    }
}
