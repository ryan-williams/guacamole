package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.reference.ReferencePosition

class BoundedIterator[T] private(startAtOpt: Option[ReferencePosition],
                                 stopAtOpt: Option[ReferencePosition],
                                 lociObjects: BufferedIterator[T])(implicit toPos: T => ReferencePosition)
  extends Iterator[T] {

  startAtOpt.foreach(startAt =>
    while (lociObjects.hasNext && lociObjects.head < startAt)
      lociObjects.next()
  )

  override def hasNext: Boolean = {
    lociObjects.hasNext &&
      (
        stopAtOpt.isEmpty ||
        stopAtOpt.exists(stopAt => lociObjects.head >= stopAt)
      )
  }

  override def next(): T =
    if (hasNext)
      lociObjects.next()
    else
      throw new NoSuchElementException

}

object BoundedIterator {
  def apply[T](startAtOpt: Option[ReferencePosition],
               stopAtOpt: Option[ReferencePosition],
               lociObjects: Iterator[T])(implicit toPos: T => ReferencePosition): BoundedIterator[T] =
    new BoundedIterator(startAtOpt, stopAtOpt, lociObjects.buffered)
}
