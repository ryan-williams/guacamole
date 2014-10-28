package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole.TestUtil.SparkFunSuite
import org.bdgenomics.guacamole.{ DistributedUtil, LociSet, TestUtil }

class PairedEndAlignmentSVCallerSuite extends SparkFunSuite {

  sparkTest("testing sv rules") {
    val readSet = TestUtil.loadReads(sc, "NA12878-91426375-91426496.sam")
    val reads = readSet.mappedReads.collect()
    println(reads.size)
    println(reads.count(_.inDuplicatedRegion))
    val invertedRegionReads = reads.filter(_.inInvertedRegion)
    println(invertedRegionReads.size)
  }

  sparkTest("testing sv caller") {
    val readSet = TestUtil.loadReads(sc, "NA12878-91426375-91426496.sam")

    val loci = LociSet.parse("1:91426375-91426496")
    val lociPartitions = DistributedUtil.partitionLociUniformly(
      1,
      loci
    )
    val genotypes = PairedEndAlignmentSVCaller.callVariantsInSample(readSet.mappedReads, lociPartitions, 0.4, 10)
    println(genotypes.count())
  }

}
