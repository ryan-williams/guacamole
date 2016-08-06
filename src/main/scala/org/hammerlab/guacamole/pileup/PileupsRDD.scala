package org.hammerlab.guacamole.pileup

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.readsets.iterator.FilterAndRequireLoci
import org.hammerlab.guacamole.readsets.iterator.overlaps.{LociOverlapsIterator, LociOverlapsPerSampleIterator}
import org.hammerlab.guacamole.readsets.{NumSamples, PartitionedReads, PerSample}
import org.hammerlab.guacamole.reference.{Position, ReferenceGenome}

class PileupsRDD(partitionedReads: PartitionedReads) {

  def sc = partitionedReads.sc

  def pileups(reference: ReferenceGenome,
              requiredLoci: LociSet = LociSet()): RDD[Pileup] = {

    val requiredLociBroadcast: Broadcast[LociSet] = sc.broadcast(requiredLoci)

    partitionedReads
      .mapPartitions(
        (reads, loci) => {

          val requiredLoci = requiredLociBroadcast.value

          val overlaps =
            FilterAndRequireLoci[MappedRead, Iterable[MappedRead]](
              reads,
              loci,
              requiredLoci,
              LociOverlapsIterator(halfWindowSize = 0, _),
              empty = Nil
            )

          for {
            (Position(contigName, locus), overlappingReads) <- overlaps
          } yield {
            Pileup(
              overlappingReads,
              contigName,
              locus,
              reference.getContig(contigName)
            )
          }
        }
      )
  }

  def perSamplePileups(numSamples: NumSamples,
                       reference: ReferenceGenome,
                       requiredLoci: LociSet = LociSet()): RDD[PerSample[Pileup]] = {

    val requiredLociBroadcast: Broadcast[LociSet] = sc.broadcast(requiredLoci)

    partitionedReads
      .mapPartitions(
        (reads, loci) => {

          val requiredLoci = requiredLociBroadcast.value

          val overlaps =
            FilterAndRequireLoci[MappedRead, PerSample[Iterable[MappedRead]]](
              reads,
              loci,
              requiredLoci,
              LociOverlapsPerSampleIterator(numSamples, halfWindowSize = 0, _),
              empty = Vector.fill(numSamples)(Nil)
            )

          for {
            (Position(contigName, locus), reads) <- overlaps
          } yield
            for {
              (sampleReads, sampleId) <- reads.zipWithIndex
              referenceContig = reference.getContig(contigName)
            } yield
              Pileup(
                sampleReads,
                contigName,
                locus,
                referenceContig
              )
        }
      )
  }
}

object PileupsRDD {
  implicit def partitionedReadsToPileupsRDD(partitionedReads: PartitionedReads): PileupsRDD =
    new PileupsRDD(partitionedReads)
}
