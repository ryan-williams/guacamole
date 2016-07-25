package org.hammerlab.guacamole.pileup

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.pileup.PileupsRDD.SampleRead
import org.hammerlab.guacamole.reads.{MappedRead, SampleRegion}
import org.hammerlab.guacamole.readsets.iterator.FilterAndRequireLoci
import org.hammerlab.guacamole.readsets.iterator.overlaps.{LociOverlapsIterator, LociOverlapsPerSampleIterator}
import org.hammerlab.guacamole.readsets.{PartitionedReads, PerSample}
import org.hammerlab.guacamole.reference.{Position, ReferenceGenome}

class PileupsRDD(partitionedReads: PartitionedReads) {

  def sc = partitionedReads.sc

  def pileups(reference: ReferenceGenome,
              requiredLoci: LociSet = LociSet()): RDD[Pileup] = {

    val requiredLociBroadcast: Broadcast[LociSet] = sc.broadcast(requiredLoci)

    partitionedReads
      .mapPartitions(
        (keyedReads, loci) => {

          val requiredLoci = requiredLociBroadcast.value

          val overlaps =
            FilterAndRequireLoci[MappedRead, Iterable[MappedRead]](
              keyedReads.map(_._2),
              loci,
              requiredLoci,
              LociOverlapsIterator(halfWindowSize = 0, _),
              empty = Nil
            )

          for {
            (Position(contigName, locus), overlappingReads) <- overlaps
          } yield
            Pileup(
              overlappingReads,
              contigName,
              locus,
              reference.getContig(contigName)
            )
        }
      )
  }

  def perSamplePileups(sampleNames: PerSample[String],
                       reference: ReferenceGenome,
                       requiredLoci: LociSet = LociSet()): RDD[PerSample[Pileup]] = {

    val requiredLociBroadcast: Broadcast[LociSet] = sc.broadcast(requiredLoci)

    val numSamples = sampleNames.length

    partitionedReads
      .mapPartitions(
        (keyedReads, loci) => {

          val requiredLoci = requiredLociBroadcast.value

          val overlaps =
            FilterAndRequireLoci[SampleRead, PerSample[Iterable[SampleRead]]](
              keyedReads.map(SampleRegion(_)),
              loci,
              requiredLoci,
              LociOverlapsPerSampleIterator(numSamples, halfWindowSize = 0, _),
              empty = Vector.fill(numSamples)(Nil)
            )

          for {
            (Position(contigName, locus), keyedReads) <- overlaps
          } yield
            for {
              (sampleReads, sampleId) <- keyedReads.zipWithIndex
              sampleName = sampleNames(sampleId)
              referenceContig = reference.getContig(contigName)
            } yield
              Pileup(sampleReads.map(_.region), contigName, locus, referenceContig)
        }
      )
  }
}

object PileupsRDD {
  implicit def partitionedReadsToPileupsRDD(partitionedReads: PartitionedReads): PileupsRDD =
    new PileupsRDD(partitionedReads)

  type SampleRead = SampleRegion[MappedRead]
}
