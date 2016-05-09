package org.hammerlab.guacamole.util

class RunLengthIterator[T] private(it: BufferedIterator[T]) extends Iterator[(T, Int)] {

  override def hasNext: Boolean = it.hasNext

  override def next(): (T, Int) = {
    val elem = it.head
    var count = 0
    while (it.hasNext && it.head == elem) {
      it.next()
      count += 1
    }
    (elem, count)
  }
}

object RunLengthIterator {
  def apply[T](it: Iterator[T]): RunLengthIterator[T] = new RunLengthIterator(it.buffered)
}
