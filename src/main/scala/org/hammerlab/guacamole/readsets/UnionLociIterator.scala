package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.reference.HasLocus
import org.hammerlab.guacamole.util.OptionIterator

/**
 *
 * @param lociObjs1
 * @param lociObjs2
 * @tparam T
 */
class UnionLociIterator[+T <: HasLocus](lociObjs1: BufferedIterator[T],
                                        lociObjs2: BufferedIterator[T])
  extends OptionIterator[T] {

  override def _advance: Option[T] = {
    (lociObjs1.hasNext, lociObjs2.hasNext) match {
      case (false, false) => None
      case ( true, false) => Some(lociObjs1.next())
      case (false,  true) => Some(lociObjs2.next())
      case ( true,  true) =>
        val (obj1, obj2) = (lociObjs1.head, lociObjs2.head)
        val (locus1, locus2) = (obj1.locus, obj2.locus)

        if (locus1 < locus2)
          Some(lociObjs1.next())
        else
          Some(lociObjs2.next())
    }
  }
}