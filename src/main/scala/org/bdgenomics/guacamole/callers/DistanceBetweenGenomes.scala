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

import org.bdgenomics.formats.avro.{ ADAMContig, ADAMVariant, ADAMGenotypeAllele, ADAMGenotype }
import org.bdgenomics.formats.avro.ADAMGenotypeAllele.{ NoCall, Ref, Alt, OtherAlt }
import org.bdgenomics.guacamole._
import org.apache.spark.SparkContext._
import scala.collection.JavaConversions
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
    @Option(name = "-k", metaVar = "K", usage = "Kmer length")
    var k: Int = 2

    @Option(name = "-smoother", metaVar = "K", usage = "Smoothing factor")
    var smoother: Int = 1

    @Option(name = "-max-reads", metaVar = "X", usage = "Max reads per sample to consider. Default (0) uses all reads.")
    var maxReads: Int = 0
  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(args, appName = Some(name))

    val readSets = Common.loadMultipleReadSetsFromArguments(args, sc, Read.InputFilters(nonDuplicate = true))
    val readRDDs: Seq[RDD[Read]] = if (args.maxReads == 0) {
      readSets.map(_.reads)
    } else {
      readSets.map(readSet => sc.parallelize(readSet.reads.take(args.maxReads)))
    }
    val reads = sc.union(readRDDs.head, readRDDs: _*)
    reads.persist()
    Common.progress("Loaded %,d files with %,d non-duplicate reads into %,d partitions.".format(
      readSets.length, reads.count, reads.partitions.length))

    val tokensAndSamples = reads.map(read => (read.token, read.sampleName)).distinct.collect.sorted
    for ((token, sample) <- tokensAndSamples) {
      Common.progress("Sample %s from file #%d".format(sample, token))
    }

    val k = args.k
    val smoother = args.smoother
    val numTokens = readSets.length
    val kmersKeyedByStringAndToken = reads.mapPartitions(readsIterator => extractKmers(k, readsIterator)).reduceByKey(_ + _)
    kmersKeyedByStringAndToken.persist()
    reads.unpersist()

    val totalsByToken = kmersKeyedByStringAndToken.map(tuple => (tuple._1._2, tuple._2)).reduceByKey(_ + _).collectAsMap()
    totalsByToken.toSeq.sorted.foreach(pair => {
      Common.progress("Total %d-mer occurrences for file #%5d: %,20d".format(k, pair._1, pair._2))
    })

    val kmersGroupedByString = kmersKeyedByStringAndToken.map(kmerTagCount =>
      (kmerTagCount._1._1, (kmerTagCount._1._2, kmerTagCount._2))).groupByKey

    kmersGroupedByString.persist()
    kmersKeyedByStringAndToken.unpersist()

    val numKmers = kmersGroupedByString.count()
    Common.progress("Total unique %d-mers: %,d (out of 4^%d=%,d possible)".format(k, numKmers, k, math.pow(4, k).toInt))

    Common.progress("Kmers: %s".format(kmersGroupedByString.keys.collect.mkString(" ")))


    val klDivergence = kmersGroupedByString.mapPartitions(iterator => {
      val result = Array.fill[Double](numTokens * numTokens)(0)
      for ((sequence, tokenCounts) <- iterator) {
        val counts = Array.fill[Long](numTokens)(0)
        for ((token, count) <- tokenCounts) {
          counts(token) = count
        }
        for (token1 <- 0 until numTokens; token2 <- 0 until numTokens) {
          def prob(token: Int): Double = (counts(token) + smoother).toDouble / (totalsByToken(token) + smoother * numKmers)
          val prob1 = prob(token1)
          val prob2 = prob(token2)
          assert(prob1 > 0)
          assert(prob2 > 0)
          result(token1 * numTokens + token2) += math.log(prob1 / prob2) * prob1
        }
      }
      Iterator(result)
    }).reduce((result1: Array[Double], result2: Array[Double]) => {
      val result = Array.fill[Double](result1.length)(0.0)
      var i = 0
      while (i < result.length) {
        result(i) = result1(i) + result2(i)
        i += 1
      }
      result
    })

    Common.progress("Done computing KL divergences.")
    for (token1 <- 0 until numTokens; token2 <- 0 until numTokens) {
      Common.progress("D(%d || %d) = %f".format(token1, token2, klDivergence(token1 * numTokens + token2)))
    }

    DelayedMessages.default.print()
  }

  def extractKmers(k: Int, reads: TraversableOnce[Read]): Iterator[((String, Int), Long)] = {
    val counts = scala.collection.mutable.HashMap[(Int, String), Long]().withDefaultValue(0L) // (token, kmer) -> count
    reads.foreach(read => {
      val sequence = Bases.basesToString(read.sequence).toUpperCase
      var end = k
      while (end < sequence.length) {
        val kmer = sequence.substring(end - k, end + 1)
        counts((read.token, kmer)) = counts((read.token, kmer)) + 1
        end += 1
      }
    })
    counts.map(kv => ((kv._1._2, kv._1._1), kv._2)).toIterator
  }
}

