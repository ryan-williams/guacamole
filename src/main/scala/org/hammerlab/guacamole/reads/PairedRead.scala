package org.hammerlab.guacamole.reads

import htsjdk.samtools.SAMRecord
import org.hammerlab.guacamole.reference.ReferenceGenome

/**
 * PairedRead is a MappedRead or UnmappedRead with the additional mate information
 * @param read Unmapped or MappedRead base read
 * @param isFirstInPair Whether the read is earlier that the the mate read
 * @param mateAlignmentProperties Alignment location of the mate if it the mate is aligned
 * @tparam T UnmappedRead or MappedRead
 */
case class PairedRead[+T <: Read](read: T,
                                  isFirstInPair: Boolean,
                                  mateAlignmentProperties: Option[MateAlignmentProperties]) extends Read {

  val isMateMapped = mateAlignmentProperties.isDefined
  override val token: Int = read.token
  override val name: String = read.name
  override val failedVendorQualityChecks: Boolean = read.failedVendorQualityChecks
  override val sampleName: String = read.sampleName
  override val baseQualities: Seq[Byte] = read.baseQualities
  override val isDuplicate: Boolean = read.isDuplicate
  override val sequence: Seq[Byte] = read.sequence
  override val isPaired: Boolean = true
  override val isMapped = read.isMapped
  override def asMappedRead = read.asMappedRead
  override val hasMdTag = read.hasMdTag
}