package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole.TestUtil
import org.bdgenomics.guacamole.TestUtil.SparkFunSuite

class SomaticBetaBinomialCallerSuite extends SparkFunSuite {

  val algorithmParameters = Array[String](
    "-minLikelihood", "17",
    "-minMapQ", "1",
    "-minReadDepth", "5",
    "-minAlternateReadDepth", "1",
    "-maxNormalAlternateReadDepth", "10",
    "-maxMappingComplexity", "100",
    "-minAlignmentForComplexity", "30",
    "-maxPercentAbnormalInsertSize", "100")

  val caller = SomaticBetaBinomialCaller

  //    val o1 = "/tmp/somatic.logodds.fn.chr20.vcf"
  //    TestUtil.deleteIfExists(o1)
  //    caller.run(
  //      Array[String](
  //        "-tumor-reads", TestUtil.testDataPath("tumor.chr20.tough.sam"),
  //        "-normal-reads", TestUtil.testDataPath("normal.chr20.tough.sam"),
  //        "-parallelism", "1",
  //        "-out", o1,
  //        "-loci", "20:49074448-49074449,20:42999694-42999695,20:33652547-33652548,20:25031215-25031216,20:44061033-44061034,20:45175149-45175150")
  //        ++ algorithmParameters)

  val o3 = "/tmp/somatic.logodds.fp.chr20.vcf"
  TestUtil.deleteIfExists(o3)

  caller.run(
    Array[String](
      "-tumor-reads", TestUtil.testDataPath("tumor.chr20.simplefp.sam"),
      "-normal-reads", TestUtil.testDataPath("normal.chr20.simplefp.sam"),
      "-parallelism", "1",
      "-out", o3,
      "-loci", "20:26211835-26211836,20:29603746-29603747,20:26140391-26140392,20:25939088-25939089")
      ++ algorithmParameters)
}
