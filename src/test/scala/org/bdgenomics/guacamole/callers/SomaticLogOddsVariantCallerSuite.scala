package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole.TestUtil
import org.bdgenomics.guacamole.TestUtil.SparkFunSuite
import org.bdgenomics.guacamole.pileup.Pileup

class SomaticLogOddsVariantCallerSuite extends SparkFunSuite {

  def loadPileup(filename: String, locus: Long = 0): Pileup = {
    val records = TestUtil.loadReads(sc, filename).mappedReads
    val localReads = records.collect
    Pileup(localReads, locus)
  }

  val algorithmParameters = Array[String](
    "-log-odds", "25",
    "-minLikelihood", "5",
    "-minMapQ", "1",
    "-lowStrandBiasLimit", "115",
    "-minReadDepth", "5",
    "-minAlternateReadDepth", "1",
    "-maxNormalAlternateReadDepth", "10",
    "-maxMappingComplexity", "100",
    "-minAlignmentForComplexity", "30",
    "-maxPercentAbnormalInsertSize", "100")

  val caller = SomaticLogOddsVariantCaller

  //    sparkTest("common false positive variants filtered for strand bias")
  //    {
  //      val falseNegativePositions = Array[Long](42999694, 33652547, 25031215, 44061033, 45175149)
  //      falseNegativePositions.flatMap(
  //        val pileup =
  //      )
  //    }

  val o1 = "/tmp/somatic.logodds.fn.chr20.vcf"
  TestUtil.deleteIfExists(o1)
  caller.run(
    Array[String](
      "-tumor-reads", TestUtil.testDataPath("tumor.chr20.tough.sam"),
      "-normal-reads", TestUtil.testDataPath("normal.chr20.tough.sam"),
      "-parallelism", "1",
      "-out", o1,
      "-loci", "20:49074448-49074449,20:42999694-42999695,20:33652547-33652548,20:25031215-25031216,20:44061033-44061034,20:45175149-45175150")
      ++ algorithmParameters)

  //  sparkTest("common false positive variants filtered for strand bias")
  //  {
  //        val o2 = "/tmp/somatic.logodds.sb.chr20.vcf"
  //        TestUtil.deleteIfExists(o2)
  //        caller.run(
  //          Array[String](
  //            "-tumor-reads", TestUtil.testDataPath("tumor.chr20.strandbias.sam"),
  //            "-normal-reads", TestUtil.testDataPath("normal.chr20.strandbias.sam"),
  //            "-parallelism", "1",
  //            "-out", o2,
  //            "-loci", "20:13720384-13720385,20:60992707-60992708,20:42248574-42248575,20:25922747-25922748")
  //            ++ algorithmParameters)
  //  }

  //    sparkTest("common false positive variants")
  //    {
  //    val o3 = "/tmp/somatic.logodds.fp.chr20.vcf"
  //    TestUtil.deleteIfExists(o3)
  //
  //    caller.run(
  //      Array[String](
  //        "-tumor-reads", TestUtil.testDataPath("tumor.chr20.simplefp.sam"),
  //        "-normal-reads", TestUtil.testDataPath("normal.chr20.simplefp.sam"),
  //        "-parallelism", "1",
  //        "-out", o3,
  //        "-loci", "20:26211835-26211836,20:29603746-29603747,20:26140391-26140392,20:25939088-25939089")
  //        ++ algorithmParameters)
  //    }
}
