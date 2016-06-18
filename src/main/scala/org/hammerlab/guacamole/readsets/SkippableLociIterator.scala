package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.reference.HasLocus
import org.hammerlab.guacamole.reference.ReferencePosition.Locus
import org.hammerlab.magic.iterator.OptionIterator

abstract class SkippableLociIterator[+T <: HasLocus] extends OptionIterator[T] {

  var locus: Locus = 0

  override def postNext(): Unit = {
    locus += 1
  }

  def skipTo(newLocus: Locus): this.type = {
    if (newLocus > locus) {
      locus = newLocus
      clear()
    }
    this
  }

}
