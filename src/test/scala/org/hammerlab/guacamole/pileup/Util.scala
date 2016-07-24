package org.hammerlab.guacamole.pileup

import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.reference.{Locus, ReferenceBroadcast}

trait Util {
//  def reference: ReferenceBroadcast

  def makePileup(reads: Iterable[MappedRead],
                 locus: Locus = 2,
                 contigName: String = "chr1")(implicit reference: ReferenceBroadcast) =
    Pileup(contigName, locus, reference.getContig(contigName), reads)

  def makePileup(reads: Iterable[MappedRead],
                 contigName: String,
                 locus: Locus)(implicit reference: ReferenceBroadcast) =
    Pileup(contigName, locus, reference.getContig(contigName), reads)
}
