/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bdgenomics.guacamole.callers

import java.io.{ BufferedWriter, OutputStreamWriter }

import com.google.common.collect.ImmutableRangeSet
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.formats.avro.{ ADAMContig, ADAMVariant, ADAMGenotypeAllele, ADAMGenotype }
import org.bdgenomics.formats.avro.ADAMGenotypeAllele.{ NoCall, Ref, Alt, OtherAlt }
import org.bdgenomics.guacamole._
import org.apache.spark.SparkContext._
import scala.collection.{ mutable, JavaConversions }
import org.kohsuke.args4j.Option
import org.bdgenomics.adam.cli.Args4j
import org.bdgenomics.guacamole.Common.Arguments._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.Logging

object DistanceBetweenGenomes extends Command with Serializable with Logging {
  override val name = "distance"
  override val description = "call variants using a simple threshold"

  private class Arguments extends Base with MultipleReadSets with DistributedUtil.Arguments {
    @Option(name = "-k-min", metaVar = "K", usage = "Kmer length min (inclusive)")
    var kMin: Int = 8

    @Option(name = "-k-max", metaVar = "K", usage = "Kmer length max (exclusive)")
    var kMax: Int = 10

    @Option(name = "-prior-q", metaVar = "K", usage = "Smoothing quality factor")
    var priorQuality: Double = 3.0

    @Option(name = "-max-files", metaVar = "X", usage = "Max files to consider. Default (0) uses all files.")
    var maxFiles: Int = 0

    @Option(name = "-num-partitions", metaVar = "X", usage = "Number of spark partitions to use. Default (0) does no repartitioning.")
    var numPartitions: Int = 0

    @Option(name = "-out", metaVar = "X", usage = "Output file in csv format.")
    var out: String = _
  }

  def phredToProbability(phred: Double) = math.pow(10, phred * -1 / 10.0)

  val storageLevel = StorageLevel.MEMORY_ONLY

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(args, appName = Some(name))

    val kValues = (args.kMin until args.kMax).toSeq
    val smoothers = kValues.map(k => (k, math.pow(1 - phredToProbability(args.priorQuality), k))).toMap
    Common.progress("Will write results to: %s".format(args.out))
    Common.progress("K = %s".format(kValues.mkString(", ")))
    Common.progress("Smoothing by: %f .. %f".format(smoothers(kValues.min), smoothers(kValues.max)))

    val readSetsAll = Common.loadMultipleReadSetsFromArguments(args, sc, Read.InputFilters(nonDuplicate = true))
    val readSets = if (args.maxFiles == 0) readSetsAll else readSetsAll.take(args.maxFiles)
    val readRDDs: Seq[RDD[Read]] = readSets.map(_.reads)

    val readsUnioned = sc.union(readRDDs.head, readRDDs: _*)
    val reads = if (args.numPartitions > 0) readsUnioned.repartition(args.numPartitions) else readsUnioned
    reads.persist(storageLevel)
    Common.progress("Loaded %,d files with %,d non-duplicate reads into %,d partitions.".format(
      readSets.length, reads.count, reads.partitions.length))

    val tokensAndSamples = reads.map(read => (read.token, read.sampleName)).distinct.collect.sorted
    for ((token, sample) <- tokensAndSamples) {
      Common.progress("Sample %s from file #%d = %s".format(sample, token, readSets(token).source))
    }

    val numSamples = readSets.length
    val kmersKeyedByStringAndToken = reads.mapPartitions(readsIterator => extractKmers(kValues, readsIterator)).reduceByKey(_ + _)
    kmersKeyedByStringAndToken.persist(storageLevel)
    reads.unpersist()

    val totalsByKAndToken = kmersKeyedByStringAndToken.map(
      tuple => ((tuple._1._1.length, tuple._1._2), tuple._2)).reduceByKey(_ + _).collectAsMap()
    totalsByKAndToken.toSeq.sorted.foreach(pair => {
      Common.progress("Total weight for file #%5d at k=%d: %f".format(pair._1._2, pair._1._1, pair._2))
    })

    val kmersGroupedByString = kmersKeyedByStringAndToken.map(kmerTagCount =>
      (kmerTagCount._1._1, (kmerTagCount._1._2, kmerTagCount._2))).groupByKey

    kmersGroupedByString.persist(storageLevel)
    kmersKeyedByStringAndToken.unpersist()

    val numKmersOfLength = kmersGroupedByString.map(_._1.length).countByValue

    Common.progress("Total unique kmers: %,d".format(numKmersOfLength.values.sum))
    numKmersOfLength.foreach(pair => {
      val k = pair._1
      val count = pair._2
      Common.progress("Total unique %d-mers (out of 4^%d=%,d possible): %,d".format(k, k, math.pow(4, k).toInt, count))
    })

    val klDivergences = kmersGroupedByString.mapPartitions(iterator => {
      val results = kValues.map(k => (k, Array.fill[Double](numSamples * numSamples)(0))).toMap
      for ((sequence, tokenCounts) <- iterator) {
        val k = sequence.length
        val counts = Array.fill[Double](numSamples)(0)
        for ((token, count) <- tokenCounts) {
          counts(token) = count
        }
        for (token1 <- 0 until numSamples; token2 <- 0 until numSamples) {
          def probability(token: Int): Double = {
            val result = (counts(token) + smoothers(k)) / (totalsByKAndToken((k, token)) + smoothers(k) * numKmersOfLength(k))
            assert(result > 0, "Invalid prob for file %d: %f. Count=%f total=%f".format(
              token, result, counts(token), totalsByKAndToken((k, token))))
            result
          }
          val prob1 = probability(token1)
          val prob2 = probability(token2)
          results(k)(token1 * numSamples + token2) += math.log(prob1 / prob2) * prob1
        }
      }
      Iterator(results)
    }).reduce((results1: Map[Int, Array[Double]], results2: Map[Int, Array[Double]]) => {
      val results = kValues.map(k => (k, Array.fill[Double](numSamples * numSamples)(0))).toMap
      results.keys.foreach(k => {
        var i = 0
        while (i < results(k).length) {
          results(k)(i) = results1(k)(i) + results2(k)(i)
          i += 1
        }
      })
      results
    })

    Common.progress("Done computing KL divergences.")
    kValues.foreach(k => {
      for (token1 <- 0 until numSamples; token2 <- 0 until numSamples) {
        Common.progress("D(%d || %d) at k=%d = %f".format(token1, token2, k, klDivergences(k)(token1 * numSamples + token2)))
      }
    })

    if (args.out.nonEmpty) {
      val filesystem = FileSystem.get(new Configuration())
      val path = new Path(args.out)
      val writer = new BufferedWriter(new OutputStreamWriter(filesystem.create(path, true)))
      writer.write("K, Index 1, File 1, Index 2, File 2, Distance\n")
      kValues.foreach(k => {
        for (token1 <- 0 until numSamples; token2 <- 0 until numSamples) {
          writer.write("%d, %d, %s, %d, %s, %f\n".format(
            k,
            token1,
            readSets(token1).source,
            token2,
            readSets(token2).source,
            klDivergences(k)(token1 * numSamples + token2)))
        }
      })
      writer.close()
      Common.progress("Wrote: %s".format(args.out))
    }
    DelayedMessages.default.print()
  }

  // kValues must be sorted
  def extractKmers(kValues: Seq[Int], reads: TraversableOnce[Read]): Iterator[((String, Int), Double)] = {
    val counts = scala.collection.mutable.HashMap[(Int, String), Double]().withDefaultValue(0) // (token, kmer) -> count
    reads.foreach(read => {
      val sequence = Bases.basesToString(read.sequence).toUpperCase
      val sequenceProbabilities = read.baseQualities.map(phred => 1 - phredToProbability(phred))
      val skipIndices = sequence.zip(sequenceProbabilities).zipWithIndex.filter(pair => {
        val base: Char = pair._1._1
        val prob: Double = pair._1._2
        (base != 'A' && base != 'T' && base != 'C' && base != 'G') || (!(prob > 0))
      }).map(_._2).toList
      kValues.foreach(k => {
        var nextSkipIndex = skipIndices
        var start = 0 // kmer is subsequence from start to start + k
        while (start + k <= sequence.length) {
          if (nextSkipIndex.nonEmpty && start + k > nextSkipIndex.head) {
            // Kmer has a nonstandard base (e.g. "N") or quality 0 at this index. Skip ahead.
            start = math.max(start, nextSkipIndex.head)
            nextSkipIndex = nextSkipIndex.tail
          } else {
            val kmer = sequence.substring(start, start + k)
            val probability = sequenceProbabilities.slice(start, start + k).product
            assert(probability > 0)
            counts((read.token, kmer)) += probability
          }
          start += 1
        }
      })
    })
    counts.map(kv => ((kv._1._2, kv._1._1), kv._2)).toIterator
  }
}

