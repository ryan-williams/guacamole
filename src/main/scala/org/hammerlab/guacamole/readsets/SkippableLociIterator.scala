package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.reference.HasLocus
import org.hammerlab.guacamole.reference.ReferencePosition.Locus
import org.hammerlab.guacamole.util.OptionIterator

abstract class SkippableLociIterator[T <: HasLocus] extends OptionIterator[T] {

  var locus: Locus = 0

  override def postNext(n: T): Unit = {
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

//abstract class SkippableLociKeyedIterator[T] extends SkippableLociIteratorBase[(Locus, T)](_._1)
//abstract class SkippableLociIterator extends SkippableLociIteratorBase[Locus](x => x)
