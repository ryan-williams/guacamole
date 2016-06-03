package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.reference.{Contig, ReferencePosition}
import org.hammerlab.guacamole.reference.ReferencePosition.Locus
import org.hammerlab.guacamole.util.OptionIterator

abstract class SkippableLociIterator[T] extends OptionIterator[(Locus, T)] {

  var locus: Locus = 0

  override def postNext(n: (Locus, T)): Unit = {
    locus += 1
  }

  def skipTo(newLocus: Locus): this.type = {
    if (newLocus > locus) {
      locus = newLocus
      _next = None
    }
    this
  }

}
