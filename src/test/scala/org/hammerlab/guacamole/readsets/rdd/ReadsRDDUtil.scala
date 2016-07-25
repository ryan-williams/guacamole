package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.{MappedRead, ReadsUtil}
import org.hammerlab.guacamole.reference.{ContigName, Locus}

import scala.collection.mutable

trait ReadsRDDUtil extends ReadsUtil {

  def sc: SparkContext

  def makeReadsRDD(reads: (String, String, Int)*): RDD[MappedRead] =
    sc.parallelize(
      for {
        (seq, cigar, start) <- reads
      } yield
        makeRead(seq, cigar, start)
    )
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
