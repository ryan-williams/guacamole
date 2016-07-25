package org.hammerlab.guacamole.readsets.rdd

import org.apache.hadoop.fs.Path
import org.hammerlab.guacamole.loci.partitioning.LociPartitionerArgs
import org.hammerlab.magic.args4j.StringOptionHandler
import org.kohsuke.args4j.spi.BooleanOptionHandler
import org.kohsuke.args4j.{Option => Args4JOption}

trait PartitionedRegionsArgs extends LociPartitionerArgs {
  @Args4JOption(
    name = "--partitioning-dir",
    usage = "Directory from which to read an existing partition-reads RDD, with accompanying LociMap partitioning.",
    forbids = Array("--partitioned-reads-path", "--loci-partitioning-path"),
    handler = classOf[StringOptionHandler]
  )
  private var partitioningDirOpt: Option[String] = None

  @Args4JOption(
    name = "--partitioned-reads",
    usage = "Directory from which to read an existing partition-reads RDD, with accompanying LociMap partitioning.",
    forbids = Array("--partitioning-dir"),
    handler = classOf[StringOptionHandler]
  )
  private var _partitionedReadsPathOpt: Option[String] = None

  def partitionedReadsPathOpt: Option[String] =
    _partitionedReadsPathOpt
    .orElse(
      partitioningDirOpt
      .map(
        new Path(_, "reads").toString
      )
    )

  override def lociPartitioningPathOpt: Option[String] =
    _lociPartitioningPathOpt
      .orElse(
        partitioningDirOpt
        .map(
          new Path(_, "partitioning").toString
        )
      )

  @Args4JOption(
    name = "--compress",
    usage = "Whether to compress the output partitioned reads (default: true).",
    handler = classOf[BooleanOptionHandler]
  )
  var compressReadPartitions: Boolean = false
}
