package org.bdgenomics.guacamole.benchmarks

import org.bdgenomics.guacamole.pileup.Pileup
import org.bdgenomics.guacamole.{LociSet, DistributedUtil, TestUtil}
import org.bdgenomics.guacamole.TestUtil.HasSparkContext
import org.scalameter.api._

object PileupBenchmark extends PerformanceTest.Quickbenchmark with HasSparkContext {

  val loci = Gen.range("loci")(100, 1000, 100)
  val arrays: Gen[String] = for (l <- loci) yield ("20:1-" + l.toString)

  createSpark("PileupBenchmark", true)

  val tumorReads = TestUtil.loadReads(sc, "tumor.chr20.tough.sam")

  performance of "DistributedUtil" in {
    measure method "pileupFlatMap" in {
      using(arrays) in {
        loci => {
          DistributedUtil.pileupFlatMap[Pileup](
            tumorReads.mappedReads,
            DistributedUtil.partitionLociUniformly(4, LociSet.parse(loci)),
            false, // don't skip empty pileups
            pileup => Seq(pileup).iterator).collect()
        }
      }
    }
  }
}
