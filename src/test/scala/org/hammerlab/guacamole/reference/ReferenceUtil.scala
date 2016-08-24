package org.hammerlab.guacamole.reference

import org.apache.spark.SparkContext
import org.hammerlab.guacamole.readsets.ContigLengths
import org.hammerlab.guacamole.reference.ReferenceBroadcast.MapBackedReferenceSequence
import org.hammerlab.guacamole.util.Bases

import scala.collection.mutable

trait ReferenceUtil {

  def sc: SparkContext

  /**
   * Make a ReferenceBroadcast containing the specified sequences to be used in tests.
   *
   * @param sc
   * @param contigStartSequences tuples of (contig name, start, reference sequence) giving the desired sequences
   * @param contigLengths total length of each contigs (for simplicity all contigs are assumed to have the same length)
   * @return a map acked ReferenceBroadcast containing the desired sequences
   */
  def makeReference(sc: SparkContext,
                    contigStartSequences: Seq[(ContigName, Int, String)],
                    contigLengths: Int = 1000): ReferenceBroadcast = {

    val map = mutable.HashMap[String, ContigSequence]()

    for {
      (contig, start, sequence) <- contigStartSequences
    } {
      val locusToBase: Map[Int, Byte] =
        (for {
          (base, locus) <- Bases.stringToBases(sequence).zipWithIndex
        } yield
          (locus + start) -> base
        ).toMap

      map(contig) = MapBackedReferenceSequence(contigLengths, sc.broadcast(locusToBase))
    }

    new ReferenceBroadcast(map.toMap, source = Some("test_values"))
  }

  def makeReference(contigLengths: ContigLengths,
                    refStrs: (String, Int, String)*): ReferenceBroadcast = {

    val refsMap = mutable.Map[String, mutable.Map[Int, Byte]]()

    for {
      (contig, pos, sequence) <- refStrs
      (base, idx) <- sequence.zipWithIndex
    } {
      refsMap
        .getOrElseUpdate(
          contig,
          mutable.Map[Int, Byte]()
        )
        .getOrElseUpdate(
          pos + idx,
          base.toByte
        )
    }

    ReferenceBroadcast(
      (for {
        (contig, basesMap) <- refsMap
        contigLength = contigLengths(contig)
        basesMapBroadcast = sc.broadcast(basesMap.toMap)
      } yield
        contig -> MapBackedReferenceSequence(contigLength.toInt, basesMapBroadcast)
      ).toMap
    )
  }
}
