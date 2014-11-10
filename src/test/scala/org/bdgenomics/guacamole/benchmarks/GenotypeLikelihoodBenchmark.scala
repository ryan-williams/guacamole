package org.bdgenomics.guacamole.benchmarks

import org.bdgenomics.guacamole.TestUtil
import org.bdgenomics.guacamole.TestUtil._
import org.scalameter.PerformanceTest
import org.scalameter.api._

class GenotypeLikelihoodBenchmark extends PerformanceTest.Quickbenchmark with HasSparkContext {
  val loci = Gen.range("loci")(100, 1000, 100)
  val arrays: Gen[String] = for (l <- loci) yield ("20:1-" + l.toString)

  createSpark("PileupBenchmark", true)

  val tumorReads = TestUtil.loadReads(sc, "tumor.chr20.tough.sam")
  val overlapLociReads = tumorReads.mappedReads.filter(_.overlapsLocus(3453543))


  performance of "Allele" in {
    measure method "computeLikelihoods" in {

    }
  }

}
