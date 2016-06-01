package org.hammerlab.guacamole.util

trait SkippableOptionIterator[T, U] extends OptionIterator[T] {
  def _skipTo(u: U): Unit
  def skipTo(u: U): this.type = {
    _skipTo(u)
    this
  }
}
