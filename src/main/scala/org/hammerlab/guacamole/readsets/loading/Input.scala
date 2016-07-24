package org.hammerlab.guacamole.readsets.loading

import org.hammerlab.guacamole.readsets.SampleId

class Input(val id: SampleId, val sampleName: String, val path: String) extends Serializable

object Input {
  def apply(id: SampleId, sampleName: String, path: String): Input = new Input(id, sampleName, path)
  def unapply(input: Input): Option[(SampleId, String, String)] = Some((input.id, input.sampleName, input.path))
}
