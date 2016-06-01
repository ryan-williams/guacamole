package org.hammerlab.guacamole.reference
import org.hammerlab.guacamole.reference.ReferencePosition.Locus

import scala.math.PartiallyOrdered

case class ReferencePosition(contig: Contig, locus: Locus)
  extends ReferenceRegion
    with PartiallyOrdered[ReferencePosition] {

  override def tryCompareTo[B >: ReferencePosition](that: B)(implicit ev: (B) => PartiallyOrdered[B]): Option[Int] = {
    that match {
      case other: ReferencePosition =>
        if (contig == other.contig)
          Some(locus.compare(other.locus))
        else
          None
      case _ => None
    }
  }

  def start = locus
  def end = locus + 1

  def +(length: Locus): ReferencePosition = ReferencePosition(contig, locus + length)
  def -(length: Locus): ReferencePosition = ReferencePosition(contig, math.max(0L, locus - length))

  override def toString: String = s"$contig:$locus"
}

object ReferencePosition {
  implicit val ordering = new Ordering[ReferencePosition] {
    override def compare(x: ReferencePosition, y: ReferencePosition): Int = {
      val contigCmp = x.contig.compare(y.contig)
      if (contigCmp == 0)
        x.locus.compare(y.locus)
      else
        contigCmp
    }
  }

  type Locus = Long
}
