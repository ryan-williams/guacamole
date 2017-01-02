package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.rdd.RDD
import org.hammerlab.genomics.loci.set.test.LociSetUtil
import org.hammerlab.genomics.reads.{ MappedRead, ReadsUtil }
import org.hammerlab.guacamole.util.GuacFunSuite

class MicroRegionPartitionerSuite
  extends GuacFunSuite
    with LociSetUtil
    with ReadsUtil {

  test("partition") {

    def pairsToReads(pairs: Seq[(Int, Int)]): RDD[MappedRead] =
      sc.parallelize(
        for {
          (start, length) <- pairs
        } yield
          makeRead(
            sequence = "A" * length.toInt,
            cigar = "%sM".format(length),
            start = start
          )
      )

    val reads =
      pairsToReads(
        Seq(
          (5, 1),
          (6, 1),
          (7, 1),
          (8, 1)
        )
      )

    val result =
      new MicroRegionPartitioner(
        reads,
        numPartitions = 2,
        microPartitionsPerPartition = 100
      )
      .partition("chr1:0-100")

    result.toString should === ("chr1:0-7=0,chr1:7-100=1")
  }
}
