package org.hammerlab.guacamole.reads

import org.hammerlab.guacamole.readsets.SampleId
import org.hammerlab.guacamole.reference.{ContigName, HasContig, Interval, Locus, ReferenceRegion}

case class SampleRegion[R <: ReferenceRegion](sampleId: SampleId, region: R)
  extends ReferenceRegion
  with HasSampleId {

  override def contigName: ContigName = region.contigName
  override def start: Locus = region.start
  override def end: Locus = region.end
}

object SampleRegion {
  def apply[R <: ReferenceRegion](pair: (SampleId, R)): SampleRegion[R] =
    SampleRegion(pair._1, pair._2)

  implicit def fromSampleRegion[R <: ReferenceRegion](sampleRegion: SampleRegion[R]): R = sampleRegion.region
}
