package org.hammerlab.guacamole.variants

import org.hammerlab.genomics.bases.Bases.{ BasesOrdering, _ }
import org.hammerlab.genomics.bases.{ Base, Bases }

case class Allele(refBases: Bases, altBases: Bases) extends Ordered[Allele] {

  val isVariant = refBases != altBases

  override def toString: String = "Allele(%s,%s)".format(refBases, altBases)

  override def compare(that: Allele): Int =
    BasesOrdering.compare(refBases, that.refBases) match {
      case 0 => BasesOrdering.compare(altBases, that.altBases)
      case x => x
    }
}

object Allele {
//  def apply(refBases: String, altBases: String): Allele =
//    Allele(refBases, altBases)

  def apply(refBase: Base, altBase: Base): Allele =
    Allele(Bases(refBase), Bases(altBase))
}
