package org.bdgenomics.guacamole.variants

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.bdgenomics.guacamole.Bases
import org.bdgenomics.guacamole.reads.MappedRead

case class Breakpoint(sampleName: String,
                      referenceContig: String,
                      supportingReads: Seq[MappedRead]) extends ReferenceVariant {

  lazy val translocationSupport = supportingReads.view.filter(_.inTranslocatedRegion)
  lazy val duplicationSupport = supportingReads.view.filter(_.inDuplicatedRegion)
  lazy val inversionSupport = supportingReads.view.filter(_.inInvertedRegion)

  lazy val allele: Allele = {
    if (duplicationSupport.size > inversionSupport.size) {
      Allele(Seq(Bases.T), Bases.stringToBases("<DUP>"))
    } else {
      Allele(Seq(Bases.T), Bases.stringToBases("<INV>"))
    }
  }

  lazy val startStatistics = {
    val stats = new DescriptiveStatistics()
    val starts = supportingReads.map(_.start)
    val mateStarts = supportingReads
      .filter(r => r.matePropertiesOpt.exists(p => p.mateReferenceContig.exists(c => c == r.referenceContig)))
      .flatMap(_.matePropertiesOpt.flatMap(_.mateStart))

    (starts ++ mateStarts).foreach(stats.addValue(_))
    stats
  }

  lazy val start = math.round(startStatistics.getPercentile(25))
  lazy val end = math.round(startStatistics.getPercentile(75))

  lazy val minStart = math.round(startStatistics.getMin)
  lazy val maxEnd = math.round(startStatistics.getMax)

  lazy val length = (end - start + 1).toInt

  def overlapsBreakpoint(other: Breakpoint): Boolean = {
    (minStart >= other.minStart && minStart < other.maxEnd) || (maxEnd > other.minStart && maxEnd < other.maxEnd)
  }

  def callAllele: CalledAllele = {
    CalledAllele(
      sampleName,
      referenceContig,
      start,
      allele,
      AlleleEvidence(
        likelihood = 1.0,
        supportingReads.size,
        supportingReads.size,
        supportingReads.size,
        supportingReads.size,
        60,
        60
      )
    )
  }

  def merge(other: Breakpoint): Breakpoint = {

    new Breakpoint(
      sampleName,
      referenceContig,
      supportingReads ++ other.supportingReads
    )
  }
}

//class BreakpointSerializer() extends Serializer[Breakpoint] with HasAlleleSerializer {
//  override def write(kryo: Kryo, output: Output, obj: Breakpoint): Unit = {
//    output.writeString(obj.sampleName)
//    output.writeString(obj.referenceContig)
//    output.writeLong(obj.start, true)
//    alleleSerializer.write(kryo, output, obj.allele)
//
//    obj.endOpt match {
//      case Some(end) => {
//        output.writeBoolean(true)
//        output.writeLong(obj.length, true)
//      }
//      case _ => output.writeBoolean(false)
//    }
//
//    output.writeInt(obj.numberReads, true)
//    output.writeInt(obj.numberVariantReads, true)
//
//  }
//
//  override def read(kryo: Kryo, input: Input, klazz: Class[Breakpoint]): Breakpoint = {
//    val sampleName: String = input.readString()
//    val referenceContig: String = input.readString()
//    val start: Long = input.readLong(true)
//    val allele = alleleSerializer.read(kryo, input, classOf[Allele])
//
//    val hasEnd = input.readBoolean()
//    val endOpt: Option[Long] = if (hasEnd)
//      Some(input.readLong(true))
//    else
//      None
//
//    val numberReads: Int = input.readInt(true)
//    val numberVariantReads: Int = input.readInt(true)
//
//    Breakpoint(
//      sampleName,
//      referenceContig,
//      allele,
//      start,
//      endOpt,
//      numberReads,
//      numberVariantReads
//    )
//  }
//}

object Breakpoint {

  def apply(reads: Seq[MappedRead]): Breakpoint = {

    val referenceContig = reads.head.referenceContig
    val starts = reads.map(_.start)
    val mateStarts = reads
      .filter(r => r.matePropertiesOpt.exists(p => p.mateReferenceContig.exists(c => c == r.referenceContig)))
      .flatMap(_.matePropertiesOpt.flatMap(_.mateStart))

    Breakpoint(
      reads.head.sampleName,
      referenceContig,
      reads
    )
  }
}