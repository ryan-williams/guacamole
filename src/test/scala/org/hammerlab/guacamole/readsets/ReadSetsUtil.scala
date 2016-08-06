package org.hammerlab.guacamole.readsets

import org.apache.spark.SparkContext
import org.bdgenomics.adam.models.SequenceDictionary
import org.hammerlab.guacamole.jointcaller.InputCollection
import org.hammerlab.guacamole.loci.set.LociParser
import org.hammerlab.guacamole.reads.{MappedRead, Read, ReadsUtil}
import org.hammerlab.guacamole.readsets.ReadSetsUtil.TestReads
import org.hammerlab.guacamole.readsets.io.{Input, InputFilters}
import org.hammerlab.guacamole.readsets.rdd.ReadsRDD
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.util.Bases

trait ReadSetsUtil
  extends ReadsUtil
    with ContigLengthsUtil {

  def sc: SparkContext

  def makeReadSets(inputs: InputCollection, loci: LociParser): ReadSets =
    ReadSets(sc, inputs.items, filters = InputFilters(overlapsLoci = loci))

  def makeReadSets(
    reads: TestReads,
    numPartitions: Int
  )(
    implicit reference: ReferenceBroadcast,
    sequenceDictionary: SequenceDictionary
  ): (ReadSets, Seq[MappedRead]) = {
    val (readsets, allReads) = makeReadSets(Vector(reads), numPartitions)
    (readsets, allReads(0))
  }

  def makeReadSets(
    reads: PerSample[TestReads],
    numPartitions: Int
  )(
    implicit reference: ReferenceBroadcast,
    sequenceDictionary: SequenceDictionary
  ): (ReadSets, PerSample[Seq[MappedRead]]) = {
    val (readsRDDs, allReads) =
      (for {
        (sampleReads, sampleId) <- reads.zipWithIndex
      } yield {
        val mappedReads =
          for {
            (contig, start, end, num) <- sampleReads
            referenceContig = reference.getContig(contig)
            sequence = Bases.basesToString(referenceContig.slice(start, end))
            i <- 0 until num
          } yield
            makeRead(
              sequence,
              cigar = (end - start).toString + "M",
              start = start,
              chr = contig,
              sampleId = sampleId
            )

        val reads = mappedReads.map(x => x: Read)

        val rdd = sc.parallelize(reads, numPartitions)

        ReadsRDD(rdd, Input(sampleId, "test", "test")) -> mappedReads
      }).unzip

    (
      ReadSets(readsRDDs, sequenceDictionary),
      allReads.toVector
    )
  }
}

object ReadSetsUtil {
  type TestRead = (String, Int, Int, Int)
  type TestReads = Seq[TestRead]
}
