package org.hammerlab.guacamole.reference

import org.apache.spark.SparkContext
import org.hammerlab.genomics.bases.{ Base, Bases }
import org.hammerlab.genomics.reference.test.LocusUtil
import org.hammerlab.genomics.reference.{ ContigName, Locus, NumLoci }

import scala.collection.mutable

trait ReferenceUtil extends LocusUtil {

  implicit def tupleToContig(t: (String, Int, String)): (ContigName, Locus, String) = (t._1, Locus(t._2), t._3)

  /**
   * Make a ReferenceBroadcast containing the specified sequences to be used in tests.
   *
   * @param contigStartSequences tuples of (contig name, start, reference sequence) giving the desired sequences
   * @param contigLengths total length of each contigs (for simplicity all contigs are assumed to have the same length)
   * @return a map acked ReferenceBroadcast containing the desired sequences
   */
  def makeReference(sc: SparkContext,
                    contigLengths: Int,
                    contigStartSequences: (ContigName, Locus, String)*): ReferenceBroadcast = {

    val basesMap = mutable.HashMap[ContigName, mutable.Map[Locus, Base]]()

    for {
      (contigName, start, basesStr) <- contigStartSequences
      bases = Bases(basesStr)
    } {
      val contigBasesMap = basesMap.getOrElseUpdate(contigName, mutable.Map())
      for {
        (base, offset) <- bases.zipWithIndex
        locus = start + offset
      } {
        contigBasesMap(locus) = base
      }
    }

    val contigsMap =
      for {
        (contigName, contigBasesMap) <- basesMap.toMap
      } yield
        contigName ->
          MapBackedReferenceSequence(
            contigName,
            NumLoci(contigLengths),
            sc.broadcast(contigBasesMap.toMap)
          )

    new ReferenceBroadcast(contigsMap, source = Some("test_values"))
  }

  def makeReference(sc: SparkContext, contigStartSequences: (ContigName, Locus, String)*): ReferenceBroadcast =
    makeReference(sc, 1000, contigStartSequences: _*)

  def makeReference(sc: SparkContext, contigName: ContigName, start: Locus, sequence: String): ReferenceBroadcast =
    makeReference(sc, (contigName, start, sequence))
}
