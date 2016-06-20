package org.hammerlab.guacamole.util

import java.io.{BufferedReader, IOException, InputStream, InputStreamReader}

import org.hammerlab.magic.iterator.OptionIterator

class LinesIterator(br: BufferedReader) extends OptionIterator[String] {
  override def _advance: Option[String] = {
    try {
      Some(br.readLine())
    } catch {
      case e: IOException => {
        br.close()
        None
      }
    }
  }
}

object LinesIterator {
  def apply(is: InputStream): LinesIterator =
    new LinesIterator(
      new BufferedReader(
        new InputStreamReader(
          is
        )
      )
    )
}
