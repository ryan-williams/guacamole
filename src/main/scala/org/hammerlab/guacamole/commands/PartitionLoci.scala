package org.hammerlab.guacamole.commands

import org.apache.spark.SparkContext
import org.hammerlab.guacamole.loci.partitioning.{AllLociPartitionerArgs, ApproximatePartitioner, ApproximatePartitionerArgs}
import org.hammerlab.guacamole.readsets.ReadSets
import org.kohsuke.args4j.{Option => Args4jOption}

class PartitionLociArgs extends ReadSets.Arguments with AllLociPartitionerArgs {
  @Args4jOption(
    name = "--half-window",
    usage = "Partitions get assigned all reads that overlap any base within this distance of either end of its range."
  )
  var halfWindow: Int = 50
}

object PartitionLoci extends SparkCommand[PartitionLociArgs] {
  override val name: String = "partition-loci"
  override val description: String = "Partition some loci and output statistics about them"

  override def run(args: PartitionLociArgs, sc: SparkContext): Unit = {

    val readsets = ReadSets(sc, args.pathsAndSampleNames)
    val mappedReads = readsets.mappedReadsRDDs
    val ReadSets(_, sequenceDictionary, contigLengths) = readsets

    val loci = args.parseLoci(sc.hadoopConfiguration).result(contigLengths)

    val partitioning = ApproximatePartitioner(readsets.allMappedReads, args.halfWindow, args).partition(loci)

    val partitioningBroadcast = sc.broadcast(partitioning)

    val readsRDD = sc.union(mappedReads)

    val half = args.halfWindow

    val taskReadCountsMap =
      readsRDD.flatMap(r => {
        val contig = partitioningBroadcast.value.onContig(r.contig)
        contig.getAll(r.start - half, r.end + half)
      }).countByValue()

    val taskReadCounts = taskReadCountsMap.toArray.sortBy(_._2)

    println(s"Task Read Counts:\n${taskReadCounts.map(t => s"${t._1}\t${t._2}").mkString("\n")}")
  }
}
