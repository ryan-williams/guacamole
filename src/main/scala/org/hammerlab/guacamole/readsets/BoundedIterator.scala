package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.reference.ReferencePosition

class BoundedIterator[T] private(startAtOpt: Option[ReferencePosition],
                                 stopAtOpt: Option[ReferencePosition],
                                 lociObjects: BufferedIterator[(ReferencePosition, T)])
  extends Iterator[(ReferencePosition, T)] {

  startAtOpt.foreach(startAt =>
    while (lociObjects.hasNext && lociObjects.head._1.startPos < startAt)
      lociObjects.next()
  )

  override def hasNext: Boolean = {
    lociObjects.hasNext && !stopAtOpt.exists(stopAt => lociObjects.head._1.startPos >= stopAt)
  }

  override def next(): (ReferencePosition, T) =
    if (hasNext)
      lociObjects.next()
    else
      throw new NoSuchElementException

}

object BoundedIterator {
  def apply[T](startAtOpt: Option[ReferencePosition],
               stopAtOpt: Option[ReferencePosition],
               lociObjects: Iterator[(ReferencePosition, T)]): BoundedIterator[T] =
    new BoundedIterator(startAtOpt, stopAtOpt, lociObjects.buffered)
}
