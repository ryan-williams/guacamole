package org.hammerlab.guacamole.reference
import scala.math.PartiallyOrdered

case class ReferencePosition(contig: Contig, pos: Long)
  extends ReferenceRegion
    with PartiallyOrdered[ReferencePosition] {

  override def tryCompareTo[B >: ReferencePosition](that: B)(implicit ev: (B) => PartiallyOrdered[B]): Option[Int] = {
    that match {
      case other: ReferencePosition =>
        if (contig == other.contig)
          Some(pos.compare(other.pos))
        else
          None
      case _ => None
    }
  }

  def referenceContig = contig
  def start = pos
  def end = pos + 1

  def +(length: Int): ReferencePosition = ReferencePosition(contig, pos + length)
  def -(length: Int): ReferencePosition = ReferencePosition(contig, math.max(0L, pos - length))

  override def toString: String = s"$contig:$pos"
}

object ReferencePosition {
  implicit val ordering = new Ordering[ReferencePosition] {
    override def compare(x: ReferencePosition, y: ReferencePosition): Int = {
      val contigCmp = x.contig.compare(y.contig)
      if (contigCmp == 0)
        x.pos.compare(y.pos)
      else
        contigCmp
    }
  }
}
