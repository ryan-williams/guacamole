package org.hammerlab.guacamole.pileup

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.readsets.{PartitionedReads, PerSample}
import org.hammerlab.guacamole.readsets.iterator.overlaps.PositionRegionsIterator
import org.hammerlab.guacamole.readsets.iterator.overlaps.per_sample.PositionRegionsPerSampleIterator
import org.hammerlab.guacamole.reference.{Position, ReferenceGenome}

class PileupsRDD(partitionedReads: PartitionedReads) {

  def sc = partitionedReads.sc
  def lociSetsRDD = partitionedReads.lociSetsRDD

  def pileups(reference: ReferenceGenome,
              forceCallLoci: LociSet = LociSet()): RDD[Pileup] = {

    val forceCallLociBroadcast: Broadcast[LociSet] = sc.broadcast(forceCallLoci)

    assert(
      partitionedReads.regions.getNumPartitions == lociSetsRDD.getNumPartitions,
      s"reads partitions: ${partitionedReads.regions.getNumPartitions}, loci partitions: ${lociSetsRDD.getNumPartitions}"
    )

    val numLoci = sc.accumulator(0L, "numLoci")

    partitionedReads
      .regions
      .zipPartitions(
        lociSetsRDD,
        preservesPartitioning = true
      )(
        (reads, lociIter) => {
          val loci = lociIter.next()
          if (lociIter.hasNext) {
            throw new Exception(s"Expected 1 LociSet, found ${1 + lociIter.size}.\n$loci")
          }

          numLoci += loci.count

          val windowedReads =
            new PositionRegionsIterator(
              halfWindowSize = 0,
              loci,
              forceCallLociBroadcast.value,
              reads.buffered
            )

          for {
            (Position(contig, locus), reads) <- windowedReads
          } yield {
            Pileup("all", contig, locus, reference.getContig(contig), reads)
          }
        }
      )
  }

  def perSamplePileups(sampleNames: PerSample[String],
                       reference: ReferenceGenome,
                       forceCallLoci: LociSet = LociSet()): RDD[PerSample[Pileup]] = {
    val forceCallLociBroadcast: Broadcast[LociSet] = sc.broadcast(forceCallLoci)

    assert(
      partitionedReads.regions.getNumPartitions == lociSetsRDD.getNumPartitions,
      s"reads partitions: ${partitionedReads.regions.getNumPartitions}, loci partitions: ${lociSetsRDD.getNumPartitions}"
    )

    val numLoci = sc.accumulator(0L, "numLoci")

    partitionedReads
      .regions
      .zipPartitions(
        lociSetsRDD,
        preservesPartitioning = true
      )(
        (reads, lociIter) => {
          val loci = lociIter.next()
          if (lociIter.hasNext) {
            throw new Exception(s"Expected 1 LociSet, found ${1 + lociIter.size}.\n$loci")
          }

          numLoci += loci.count

          val windowedReads =
            new PositionRegionsPerSampleIterator(
              halfWindowSize = 0,
              sampleNames.length,
              loci,
              forceCallLociBroadcast.value,
              reads.buffered
            )

          for {
            (Position(contig, locus), reads) <- windowedReads
          } yield {
            for {
              (sampleReads, sampleId) <- reads.zipWithIndex
              sampleName = sampleNames(sampleId)
            } yield
              Pileup(sampleName, contig, locus, reference.getContig(contig), sampleReads)
          }
        }
      )
  }
}

object PileupsRDD {
  implicit def partitionedReadsToPileupsRDD(partitionedReads: PartitionedReads): PileupsRDD =
    new PileupsRDD(partitionedReads)
}
