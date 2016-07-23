package org.hammerlab.guacamole.readsets.loading

class Input(val sampleName: String, val path: String) extends Serializable

object Input {
  def apply(sampleName: String, path: String): Input = new Input(sampleName, path)
  def unapply(input: Input): Option[(String, String)] = Some((input.sampleName, input.path))
}
