package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.map.LociMap
import org.hammerlab.guacamole.loci.partitioning.ApproximatePartitioner.NumMicroPartitions
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.{LociPartitioning, NumPartitions, PartitionIdx}
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.{HasReferenceRegion, PerSample}
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.collection.Map

trait ApproximatePartitionerArgs extends UniformPartitionerArgs {

  /**
   * Long >= 1. Number of micro-partitions generated for each of the `numPartitions` Spark partitions that will be
   * computed. Higher values of this will result in a more exact but more expensive computation.
   * In the extreme, setting this to greater than the number of loci (per partition) will result in an exact
   * calculation.
   */
  @Args4jOption(
    name = "--partition-accuracy",
    usage = "Number of micro-partitions to use, per Spark partition, when partitioning loci (default: 250). Set to 0 to partition loci uniformly"
  )
  var partitioningAccuracy: NumMicroPartitions = 250
}

object ApproximatePartitioner extends LociPartitioner[ApproximatePartitionerArgs] {

  // Convenience types representing "micro-partitions" indices, or numbers of micro-partitions.
  // Micro-partitions can be as numerous as loci in the genome, so we use Longs to represent them.
  // Many of them can be combined into individual Spark partitions (which are Ints).
  type MicroPartitionIdx = Long
  type NumMicroPartitions = Long

  override def apply[M <: HasReferenceRegion](args: ApproximatePartitionerArgs,
                                              loci: LociSet,
                                              regionRDDs: PerSample[RDD[M]]): LociPartitioning = {
    val sc = regionRDDs(0).sparkContext
    val numPartitions =
      if (args.parallelism == 0)
        sc.defaultParallelism
      else
        args.parallelism

    apply(numPartitions, loci, args.partitioningAccuracy, regionRDDs)
  }

  /**
   * Assign loci from a LociSet to partitions, where each partition overlaps approximately the same number of "regions"
   * (reads mapped to a reference genome).
   *
   * The approach we take is:
   *
   *  (1) chop up the loci uniformly into many genomic "micro partitions."
   *
   *  (2) for each micro partition, calculate the number of regions that overlap it.
   *
   *  (3) using these counts, assign loci to real (Spark) partitions, assuming approximately uniform depth within each
   *      micro partition.
   *
   *  Some advantages of this approach are:
   *
   *  - Stages (1) and (3), which are done locally by the Spark master, are constant time with respect to the number
   *    of regions (though linear in the number of micro-partitions).
   *
   *  - Stage (2), where runtime does depend on the number of regions, is done in parallel with Spark.
   *
   *  - Accuracy vs. performance can be tuned by setting `accuracy`.
   *
   *  - Does not require a distributed sort.
   *
   * @param numPartitions Number of partitions to split reads into.
   * @param loci Only consider reads overlapping these loci.
   * @param microPartitionsPerPartition Long >= 1. Number of micro-partitions generated for each of the `numPartitions`
   *                                    Spark partitions that will be computed. Higher values of this will result in a
   *                                    more exact but more expensive computation.
   *                                    In the extreme, setting this to greater than the number of loci (per partition)
   *                                    will result in an exact calculation.
   * @param regionRDDs: region RDD 1, region RDD 2, ...
   *                    Any number RDD[ReferenceRegion] arguments giving the regions to base the partitioning on.
   * @return LociMap of locus -> partition assignments.
   */
  def apply[M <: HasReferenceRegion](numPartitions: NumPartitions,
                                     loci: LociSet,
                                     microPartitionsPerPartition: NumMicroPartitions,
                                     regionRDDs: PerSample[RDD[M]]): LociPartitioning = {

    val sc = regionRDDs(0).sparkContext

    assume(numPartitions >= 1)
    assume(loci.count > 0)
    assume(regionRDDs.nonEmpty)

    // Step 1: split loci uniformly into micro partitions.
    val numMicroPartitions: NumMicroPartitions =
      if (microPartitionsPerPartition * numPartitions < loci.count)
        microPartitionsPerPartition * numPartitions
      else
        loci.count

    progress(
      "Splitting loci by region depth among %,d Spark partitions using %,d micro partitions."
      .format(numPartitions, numMicroPartitions)
    )

    val lociToMicroPartitionMap = UniformPartitioner(numMicroPartitions, loci)
    val microPartitionToLociMap = lociToMicroPartitionMap.inverse

    progress("Done calculating micro partitions.")

    val broadcastMicroPartitions = sc.broadcast(lociToMicroPartitionMap)

    // Step 2: total up regions overlapping each micro partition. We keep the totals as an array of Longs.
    var sampleNum = 1
    val regionCounts: PerSample[Map[MicroPartitionIdx, Long]] =
      regionRDDs.map(regions => {
        progress(s"Collecting region counts for RDD $sampleNum of ${regionRDDs.length}")

        val result =
          regions
          .flatMap(region =>
            broadcastMicroPartitions.value
            .onContig(region.referenceContig)
            .getAll(region.start, region.end)
          )
          .countByValue()

        progress("RDD %d: %,d regions".format(sampleNum, result.values.sum))

        sampleNum += 1
        result
      })

    val counts: Seq[Long] = (0L until numMicroPartitions).map(i => regionCounts.map(_.getOrElse(i, 0L)).sum)

    // Step 3: assign loci to partitions, taking into account region depth in each micro partition.
    val totalRegions = counts.sum
    val regionsPerPartition = math.max(1, totalRegions / numPartitions.toDouble)

    progress(
      "Done collecting region counts. Total regions with micro partition overlaps: %,d = ~%,.0f regions per partition."
      .format(totalRegions, regionsPerPartition)
    )

    val maxIndex = counts.view.zipWithIndex.maxBy(_._1)._2

    progress(
      "Regions per micro partition: min=%,d mean=%,.0f max=%,d at %s.".format(
        counts.min,
        counts.sum.toDouble / counts.length,
        counts(maxIndex),
        microPartitionToLociMap(maxIndex)
      )
    )

    var totalRegionsAssigned = 0.0
    var partition = 0
    def regionsRemainingForThisPartition() = math.round(((partition + 1) * regionsPerPartition) - totalRegionsAssigned)

    val builder = LociMap.newBuilder[PartitionIdx]

    var microPartition = 0
    while (microPartition < numMicroPartitions) {
      var set = microPartitionToLociMap(microPartition)
      var regionsInSet = counts(microPartition)
      while (!set.isEmpty) {
        if (regionsInSet == 0) {
          // Take the whole set if there are no regions assigned to it.
          builder.put(set, partition)
          set = LociSet()
        } else {
          // If we've allocated all regions for this partition, move on to the next partition.
          if (regionsRemainingForThisPartition() == 0)
            partition += 1
          assert(regionsRemainingForThisPartition() > 0)
          assert(partition < numPartitions)

          // Making the approximation of uniform depth within each micro partition, we assign a proportional number of
          // loci and regions to the current partition. The proportion of loci we assign is the ratio of how many
          // regions we have remaining to allocate for the current partition vs. how many regions are remaining in the
          // current micro partition.

          // Here we calculate the fraction of the current micro partition we are going to assign to the current
          // partition.
          //
          // May be 1.0, in which case all loci (and therefore regions) for this micro partition will be assigned to the
          // current partition.
          val fractionToTake = math.min(1.0, regionsRemainingForThisPartition().toDouble / regionsInSet.toDouble)

          // Based on fractionToTake, we set the number of loci and regions to assign.
          // We always take at least 1 locus to ensure we continue to make progress.
          val lociToTake = math.max(1, (fractionToTake * set.count).toLong)
          val regionsToTake = (fractionToTake * regionsInSet).toLong

          // Add the new partition assignment to the builder, and update bookkeeping info.
          val (currentSet, remainingSet) = set.take(lociToTake)
          builder.put(currentSet, partition)
          totalRegionsAssigned += math.round(regionsToTake).toLong
          regionsInSet -= math.round(regionsToTake).toLong
          set = remainingSet
        }
      }
      microPartition += 1
    }
    val result = builder.result
    assert(result.count == loci.count, s"Expected ${loci.count} loci, got ${result.count}")
    result
  }

}
