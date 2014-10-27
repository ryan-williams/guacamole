package org.bdgenomics.guacamole.reads

import htsjdk.samtools.{ SAMRecord, Cigar }
import org.bdgenomics.adam.util.{ PhredUtils, MdTag }
import org.bdgenomics.guacamole.{ Bases, HasReferenceRegion }

import scala.collection.JavaConversions

/**
 * A mapped read. See the [[Read]] trait for some of the field descriptions.
 *
 * @param referenceContig the contig name (e.g. "chr12") that this read was mapped to.
 * @param alignmentQuality the mapping quality, phred scaled.
 * @param start the (0-based) reference locus that the first base in this read aligns to.
 * @param cigar parsed samtools CIGAR object.
 */
case class MappedRead(
    token: Int,
    sequence: Seq[Byte],
    baseQualities: Seq[Byte],
    isDuplicate: Boolean,
    sampleName: String,
    referenceContig: String,
    alignmentQuality: Int,
    start: Long,
    cigar: Cigar,
    mdTagString: String,
    failedVendorQualityChecks: Boolean,
    isPositiveStrand: Boolean,
    matePropertiesOpt: Option[MateProperties]) extends Read with HasReferenceRegion {

  assert(baseQualities.length == sequence.length,
    "Base qualities have length %d but sequence has length %d".format(baseQualities.length, sequence.length))

  final override lazy val getMappedReadOpt = Some(this)

  lazy val mdTag = MdTag(mdTagString, start)

  lazy val referenceString =
    try {
      mdTag.getReference(sequenceStr, cigar, start)
    } catch {
      case e: IllegalStateException => throw new CigarMDTagMismatchException(cigar, mdTag, e)
    }

  lazy val referenceBases = Bases.stringToBases(referenceString)

  lazy val alignmentLikelihood = PhredUtils.phredToSuccessProbability(alignmentQuality)

  /** Individual components of the CIGAR string (e.g. "10M"), parsed, and as a Scala buffer. */
  val cigarElements = JavaConversions.asScalaBuffer(cigar.getCigarElements)

  /**
   * The end of the alignment, exclusive. This is the first reference locus AFTER the locus corresponding to the last
   * base in this read.
   */
  val end: Long = start + cigar.getPaddedReferenceLength

  /**
   * A read can be "clipped", meaning that some prefix or suffix of it did not align. This is the start of the whole
   * read's alignment, including any initial clipped bases.
   */
  val unclippedStart = cigarElements.takeWhile(Read.cigarElementIsClipped).foldLeft(start)({
    (pos, element) => pos - element.getLength
  })

  /**
   * The end of the read's alignment, including any final clipped bases, exclusive.
   */
  val unclippedEnd = cigarElements.reverse.takeWhile(Read.cigarElementIsClipped).foldLeft(end)({
    (pos, element) => pos + element.getLength
  })

  override def toString(): String =
    "MappedRead(%d, %s, %s, %s)".format(
      start,
      cigar.toString,
      mdTagString,
      Bases.basesToString(sequence)
    )

  /**
   *
   *  In mated paired end, we expect the following pair structure
   *
   *  r1 -------> <-------- r2
   *  Where the read with earlier start position is oriented positively 5' -> 3' and its mate (which starts later)
   *  *must* be oriented negatively.
   *
   *  Translocation
   *  Reads mapped to different chromosomes
   *  r1 -------> ..... (????)
   *
   *  Inversions:
   *  Reads mapped in the same directions
   *  r1 -------> --------> r2 or r1 <------- <-------- r2
   *
   *  Tandem Duplications:
   *  Reads mapped in the incorrect directions
   *  r1 <------- --------> r2
   */

  lazy val inTranslocatedRegion: Boolean = {
    (isMapped && matePropertiesOpt.exists(!_.isMateMapped)) ||
      matePropertiesOpt.exists(_.mateReferenceContig.exists(_ != referenceContig))
  }

  lazy val inDuplicatedRegion: Boolean = {
    matePropertiesOpt match {
      case Some(mateProperties) =>
        (mateProperties.mateReferenceContig, mateProperties.mateStart) match {
          case (Some(mateReferenceContig), Some(mateStart)) => {
            referenceContig == mateReferenceContig &&
              ((start < mateStart && !isPositiveStrand) || (start > mateStart && isPositiveStrand))
          }
          case _ => false
        }

      case None => false
    }
  }

  lazy val inInvertedRegion: Boolean = {
    matePropertiesOpt match {
      case Some(mateProperties) => {
        mateProperties.isMateMapped && isPositiveStrand == mateProperties.isMatePositiveStrand
      }
      case None => false
    }
  }

}

case class MissingMDTagException(record: SAMRecord)
  extends Exception("Missing MDTag in SAMRecord: %s".format(record.toString))

case class CigarMDTagMismatchException(cigar: Cigar, mdTag: MdTag, cause: IllegalStateException)
  extends Exception("Cigar %s seems inconsistent with MD tag %s".format(cigar.toString, mdTag.toString), cause)
