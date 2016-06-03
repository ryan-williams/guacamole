package org.hammerlab.guacamole.util

trait OptionIterator[T] extends BufferedIterator[T] {

  protected var _next: Option[T] = None

  def _advance: Option[T]

  override def hasNext: Boolean = {
    if (_next.isEmpty) {
      _next = _advance
    }
    _next.nonEmpty
  }

  override def head: T = {
    if (!hasNext) throw new NoSuchElementException
    _next.get
  }

  def postNext(n: T): Unit = {}

  override def next(): T = {
    val r = head
    _next = None
    postNext(r)
    r
  }
}
