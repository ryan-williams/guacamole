package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.LociArgs
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.readsets.PartitionedRegions
import org.hammerlab.guacamole.reference.ReferenceRegion
import org.kohsuke.args4j.spi.{OptionHandler, Parameters, Setter}
import org.kohsuke.args4j.{CmdLineParser, OptionDef, Option => Args4JOption}

import scala.reflect.ClassTag

trait LociPartitionerArgs extends LociArgs

trait AllLociPartitionerArgs
  extends ApproximatePartitionerArgs
    with ExactPartitionerArgs {

  def strToOpt(str: String): Option[String] =
    if (str.isEmpty)
      None
    else
      Some(str)

//  @Args4JOption(
//    name = "--partitioning-dir",
//    usage = "Directory from which to read an existing partition-reads RDD, with accompanying LociMap partitioning.",
//    forbids = Array("--partitioned-reads-path", "--loci-partitioning-path")
//  )
//  private var _partitioningDir: String = ""

  @Args4JOption(
    name = "--partitioning-dir",
    usage = "Directory from which to read an existing partition-reads RDD, with accompanying LociMap partitioning.",
    forbids = Array("--partitioned-reads-path", "--loci-partitioning-path")
  )
  var partitioningDirOpt: Option[String] = None

  @Args4JOption(
    name = "--partitioned-reads-path",
    usage = "Directory from which to read an existing partition-reads RDD, with accompanying LociMap partitioning.",
    forbids = Array("--partitioning-dir")
  )
  private var _partitionedReadsPath: String = ""

  def partitionedReadsPathOpt: Option[String] = strToOpt(_partitionedReadsPath)

  @Args4JOption(
    name = "--loci-partitioning-path",
    usage = "Directory path within which to save the partitioned reads and accompanying LociMap partitioning.",
    forbids = Array("--partitioning-dir")
  )
  private var _lociPartitioningPath: String = ""

  def lociPartitioningPathOpt: Option[String] = strToOpt(_lociPartitioningPath)

  @Args4JOption(name = "--loci-partitioner")
  var lociPartitionerName: String = "exact"

  def getPartitioner[R <: ReferenceRegion: ClassTag](regions: RDD[R],
                                                     halfWindowSize: Int = 0): LociPartitioner = {
    if (lociPartitionerName == "exact")
      new ExactPartitioner(regions, halfWindowSize, maxReadsPerPartition): LociPartitioner
    else if (lociPartitionerName == "approximate")
      new ApproximatePartitioner(regions, halfWindowSize, parallelism, partitioningAccuracy): LociPartitioner
    else if (lociPartitionerName == "uniform")
      new UniformPartitioner(parallelism): LociPartitioner
    else
      throw new IllegalArgumentException(s"Unrecognized --loci-partitioner: $lociPartitionerName")
  }

  def getPartitionedRegions[R <: ReferenceRegion: ClassTag](loci: LociSet,
                                                            regions: RDD[R],
                                                            halfWindowSize: Int = 0): PartitionedRegions[R] =
    PartitionedRegions(
      regions,
      getPartitioner(regions, halfWindowSize).partition(loci),
      halfWindowSize
    )
}

trait LociPartitioner {
  def partition(loci: LociSet): LociPartitioning
}

object LociPartitioner {
  // Convenience types representing Spark partition indices, or numbers of Spark partitions.
  type PartitionIndex = Int
  type NumPartitions = Int
}
