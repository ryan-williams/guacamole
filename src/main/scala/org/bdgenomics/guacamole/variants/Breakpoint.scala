package org.bdgenomics.guacamole.variants

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.bdgenomics.guacamole.Bases
import org.bdgenomics.guacamole.reads.MappedRead

case class Breakpoint(sampleName: String,
                      referenceContig: String,
                      allele: Allele,
                      start: Long,
                      endOpt: Option[Long],
                      numberReads: Int,
                      numberVariantReads: Int) extends ReferenceVariant {

  val end = endOpt.getOrElse(start + 1L)
  val length = 1

  def callAllele: CalledAllele = {
    CalledAllele(
      sampleName,
      referenceContig,
      start,
      allele,
      AlleleEvidence(
        likelihood = 1.0,
        numberReads,
        numberVariantReads,
        numberReads,
        numberVariantReads,
        60,
        60
      )
    )
  }

  def merge(other: Breakpoint): Breakpoint = {
    val mergedStart = math.min(start, other.start)
    val mergedEnd: Option[Long] = (endOpt, other.endOpt) match {
      case (Some(end), Some(otherEnd)) => Some(math.max(end, otherEnd))
      case (None, None)                => None
      case (Some(end), None)           => Some(end)
      case (None, Some(end))           => Some(end)
    }

    new Breakpoint(
      sampleName,
      referenceContig,
      allele,
      mergedStart,
      mergedEnd,
      numberReads + other.numberReads,
      numberVariantReads + other.numberVariantReads)
  }
}

class BreakpointSerializer() extends Serializer[Breakpoint] with HasAlleleSerializer {
  override def write(kryo: Kryo, output: Output, obj: Breakpoint): Unit = {
    output.writeString(obj.sampleName)
    output.writeString(obj.referenceContig)
    output.writeLong(obj.start, true)
    alleleSerializer.write(kryo, output, obj.allele)

    obj.endOpt match {
      case Some(end) => {
        output.writeBoolean(true)
        output.writeLong(obj.length, true)
      }
      case _ => output.writeBoolean(false)
    }

    output.writeInt(obj.numberReads, true)
    output.writeInt(obj.numberVariantReads, true)

  }

  override def read(kryo: Kryo, input: Input, klazz: Class[Breakpoint]): Breakpoint = {
    val sampleName: String = input.readString()
    val referenceContig: String = input.readString()
    val start: Long = input.readLong(true)
    val allele = alleleSerializer.read(kryo, input, classOf[Allele])

    val hasEnd = input.readBoolean()
    val endOpt: Option[Long] = if (hasEnd)
      Some(input.readLong(true))
    else
      None

    val numberReads: Int = input.readInt(true)
    val numberVariantReads: Int = input.readInt(true)

    Breakpoint(
      sampleName,
      referenceContig,
      allele,
      start,
      endOpt,
      numberReads,
      numberVariantReads
    )
  }
}

object Breakpoint {

  def apply(reads: Seq[MappedRead]): Breakpoint = {

    val referenceContig = reads.head.referenceContig
    val starts = reads.map(_.start)
    val mateStarts = reads
      .filter(r => r.matePropertiesOpt.exists(p => p.mateReferenceContig.exists(c => c == r.referenceContig)))
      .flatMap(_.matePropertiesOpt.flatMap(_.mateStart))
    val breakpointStart = (starts ++ mateStarts).min
    val breakpointEnd = (starts ++ mateStarts).max
    val allele = Allele(Seq(Bases.T), Bases.stringToBases("<SV>"))
    Breakpoint(
      reads.head.sampleName,
      referenceContig,
      allele,
      breakpointStart,
      Some(breakpointEnd),
      numberReads = reads.size,
      numberVariantReads = reads.size
    )
  }
}