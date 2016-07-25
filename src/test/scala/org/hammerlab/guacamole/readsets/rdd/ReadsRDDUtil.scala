package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.commands.jointcaller.InputCollection
import org.hammerlab.guacamole.loci.set.LociParser
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.ReadsUtil
import org.hammerlab.guacamole.readsets.loading.{Input, InputFilters}
import org.hammerlab.guacamole.readsets.{PerSample, ReadSets}
import org.hammerlab.guacamole.reference.{ContigName, Locus, ReferenceRegion}

import scala.collection.mutable

case class TestRegion(contigName: ContigName, start: Long, end: Long) extends ReferenceRegion

trait ReadsRDDUtil extends ReadsUtil {

  def sc: SparkContext

  def makeReadsRDD(numPartitions: Int, reads: (String, Int, Int, Int)*): RDD[TestRegion] =
    sc.parallelize(makeReads(reads).toSeq, numPartitions)

  def makeReadSets(inputs: InputCollection, loci: LociParser): ReadSets =
    ReadSets(sc, inputs.items, filters = InputFilters(overlapsLoci = loci))
}

object ReadsRDDUtil {
  type TestPileup = (ContigName, Locus, String)

  // This is used as a closure in `RDD.map`s in ReadSetsSuite, so it's advantageous to keep it in its own object here
  // and not serialize ReadSetsSuite (which is not Serializable due to scalatest Matchers' `assertionsHelper`).
  def simplifyPileup(pileup: Pileup): TestPileup =
    (
      pileup.contigName,
      pileup.locus,
      {
        val reads = mutable.Map[(Long, Long), Int]()
        for {
          e <- pileup.elements
          read = e.read
          contig = read.contigName
          start = read.start
          end = read.end
        } {
          reads((start, end)) = reads.getOrElseUpdate((start, end), 0) + 1
        }

        (for {
          ((start, end), count) <- reads.toArray.sortBy(_._1._2)
        } yield
          s"[$start,$end)${if (count > 1) s"*$count" else ""}"
        ).mkString(", ")
      }
    )
}
