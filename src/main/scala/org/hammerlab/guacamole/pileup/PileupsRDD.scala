package org.hammerlab.guacamole.pileup

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.readsets.PartitionedReads
import org.hammerlab.guacamole.readsets.{NumSamples, PerSample, PositionRegionsIterator}
import org.hammerlab.guacamole.reference.{ReferenceGenome, ReferencePosition}

class PileupsRDD(partitionedReads: PartitionedReads) {

  def sc = partitionedReads.sc
  def lociSetsRDD = partitionedReads.lociSetsRDD

  def pileups(halfWindowSize: Int,
              reference: ReferenceGenome,
              forceCallLoci: LociSet = LociSet()): RDD[Pileup] = {

    val forceCallLociBroadcast: Broadcast[LociSet] = sc.broadcast(forceCallLoci)

    partitionedReads.regions.zipPartitions(lociSetsRDD, preservesPartitioning = true)((reads, lociIter) => {
      val loci = lociIter.next()
      if (lociIter.hasNext) {
        throw new Exception(s"Expected 1 LociSet, found ${1 + lociIter.size}.\n$loci")
      }

      val windowedReads =
        new PositionRegionsIterator(
          halfWindowSize,
          loci,
          forceCallLociBroadcast.value,
          reads.buffered
        )

      for {
        (ReferencePosition(contig, locus), reads) <- windowedReads
      } yield {
        Pileup(reads, contig, locus, reference.getContig(contig))
      }
    })
  }

  def perSamplePileups(numSamples: NumSamples,
                       halfWindowSize: Int,
                       reference: ReferenceGenome,
                       forceCallLoci: LociSet = LociSet()): RDD[PerSample[Pileup]] =
    pileups(halfWindowSize, reference, forceCallLoci).map(_.bySample(numSamples))
}

object PileupsRDD {
  implicit def partitionedReadsToPileupsRDD(partitionedReads: PartitionedReads): PileupsRDD =
    new PileupsRDD(partitionedReads)
}
