/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hammerlab.guacamole

import java.util.logging.Level

import org.apache.spark.{Logging, SparkContext}
import org.bdgenomics.adam.util.ParquetLogger
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.commands._
import org.hammerlab.guacamole.commands.jointcaller.SomaticJoint

/**
 * Guacamole main class.
 */
object Main extends Logging {

  /**
   * Commands (e.g. variant caller implementations) that are part of Guacamole. If you add a new command, update this.
   */
  private val commands: Seq[Command[_]] = List(
    GermlineAssemblyCaller.Caller,
    SomaticStandard.Caller,
    VariantSupport.Caller,
    VAFHistogram.Caller,
    SomaticJoint.Caller,
    PartitionLoci
  )

  private def printUsage() = {
    println("Usage: java ... <command> [other args]\n")
    println("Available commands:")
    commands.foreach(caller => {
      println("%25s: %s".format(caller.name, caller.description))
    })
    println("\nTry java ... <command> -h for help on a particular variant caller.")
  }

  /**
   * Entry point into Guacamole.
   *
   * @param args command line arguments
   */
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      printUsage()
      System.exit(1)
    }
    val commandName = args(0)
    commands.find(_.name == commandName) match {
      case Some(command) => {
        progress("Guacamole starting.")
        ParquetLogger.hadoopLoggerLevel(Level.SEVERE)  // Quiet parquet logging.
        command.run(args.drop(1))
      }
      case None => {
        println("Unknown variant caller: %s".format(commandName))
        printUsage()
        System.exit(1)
      }
    }
  }
}

trait Scratch {

  def sc: SparkContext

  val b1 = "/datasets/ovarian/ega-tim/EGAZ00001018597_2014_ICGC_IcgcOvarian_AOCS034_1DNA_7PrimaryTumour_ICGCDBPC20130205009_IlluminaIGNOutsourcing_NoCapture_Bwa_HiSeq.jpearson.header_fixed.bam"
  val b2 = "/datasets/ovarian/ega-tim/EGAZ00001018680_2014_ICGC_IcgcOvarian_AOCS034_1DNA_1NormalBlood_ICGCDBPC20130205008_IlluminaIGNOutsourcing_NoCapture_Bwa_HiSeq.jpearson.header_fixed.bam"
  val b3 = "/datasets/ovarian/ega-tim/EGAZ00001018766_2014_ICGC_IcgcOvarian_AOCS034_1DNA_12Ascites_ICGCDBPC20130205007_IlluminaIGNOutsourcing_NoCapture_Bwa_HiSeq.jpearson.bam"
  val bams = Array(b1, b2, b3)

  val reference = "/hpc/users/ahujaa01/reference_genomes/hg19-reference/ucsc.hg19.fasta"

  import org.hammerlab.guacamole.commands.jointcaller.SomaticJoint
  import org.hammerlab.guacamole.commands.jointcaller.SomaticJoint.Caller
  import org.hammerlab.guacamole.commands.jointcaller.SomaticJoint.inputsToReadSets
  import org.hammerlab.guacamole.readsets.{InputFilters, NoSequenceDictionaryArgs, PerSample, ReadSets}
  import org.hammerlab.guacamole.loci.partitioning.{ApproximatePartitioner, ApproximatePartitionerArgs, ArgsPartitioner}
  import org.hammerlab.guacamole.loci.set.LociSet

  val readsets = ReadSets(sc, bams, InputFilters.empty)
  val mappedReads = readsets.mappedReads
  val ReadSets(_, sd, cl) = readsets
  val loci = LociSet.all(cl)

  val lp = ApproximatePartitioner(1000, loci, 250, mappedReads)
  val lpb = sc.broadcast(lp)

  val mr = sc.union(mappedReads)

  val half = 50

  val trs =
    mr.map(r => {
      val contig = lpb.value.onContig(r.referenceContig)
      contig.getAll(r.start - half, r.end + half)
    }).countByValue()
}
