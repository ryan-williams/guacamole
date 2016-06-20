package org.hammerlab.guacamole.readsets

trait ContigLengthsUtil {
  def makeContigLengths(contigs: (String, Int)*): ContigLengths =
//    ContigLengths(
      (
        for ((contig, length) <- contigs)
          yield contig -> length.toLong
      ).toMap
//    )
}
