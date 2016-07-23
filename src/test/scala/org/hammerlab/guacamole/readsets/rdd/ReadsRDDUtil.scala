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
