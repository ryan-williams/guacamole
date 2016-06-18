package org.hammerlab.guacamole.util

import org.hammerlab.magic.iterator.OptionIterator

trait SkippableOptionIterator[T, U] extends OptionIterator[T] {
  def _skipTo(u: U): Unit
  def skipTo(u: U): this.type = {
    _skipTo(u)
    this
  }
}
