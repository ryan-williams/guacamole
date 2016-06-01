package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.loci.set.LociIterator
import org.hammerlab.guacamole.reference.{Interval, ReferencePosition}

class LociContigWindowIterator[I <: Interval](loci: LociIterator, regions: ContigWindowIterator[I])
  extends BufferedIterator[(ReferencePosition, Iterable[I])] {

  var _next: (ReferencePosition, Iterable[I]) = _

  override def head: (ReferencePosition, Iterable[I]) = {
    if (!advance()) throw new NoSuchElementException
    _next
  }

  def advance(): Boolean = {
    if (_next != null) return true
    if (!loci.hasNext) return false
    if (!regions.hasNext) return false

    val locus = loci.head
    val n = regions.head
    val pos = n._1.locus

    if (pos > locus) {
      loci.skipTo(pos)
      advance()
    } else if (locus > pos) {
      regions.skipTo(locus)
      advance()
    } else {
      _next = n
      loci.next()
      regions.next()
      true
    }
  }

  override def hasNext: Boolean = advance()

  override def next(): (ReferencePosition, Iterable[I]) = {
    if (!advance()) throw new NoSuchElementException
    val n = _next
    _next = null
    n
  }
}
