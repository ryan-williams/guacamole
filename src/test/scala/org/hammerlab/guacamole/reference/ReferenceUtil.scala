package org.hammerlab.guacamole.reference

import org.apache.spark.SparkContext
import org.hammerlab.guacamole.readsets.ContigLengths
import org.hammerlab.guacamole.reference.ReferenceBroadcast.MapBackedReferenceSequence

import scala.collection.mutable

trait ReferenceUtil {

  def sc: SparkContext

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
