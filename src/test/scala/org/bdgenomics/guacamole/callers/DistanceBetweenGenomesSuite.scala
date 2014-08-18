package org.bdgenomics.guacamole.callers

import org.scalatest.matchers.ShouldMatchers._
import org.bdgenomics.guacamole.TestUtil.SparkFunSuite
import org.bdgenomics.guacamole.TestUtil

class DistanceBetweenGenomesSuite extends SparkFunSuite {

  test("extract kmers: simple") {
    val result = DistanceBetweenGenomes.extractKmers(
      kValues = Seq(2),
      reads = Seq(
        TestUtil.makeUnmappedRead("TCGATCGA", qualityScores = Some(Seq(5, 5, 5, 5, 5, 5, 5, 5))))
    ).map(pair => (pair._1._1, pair._2)).toMap
    println(result.mkString(", "))
    val weight = math.pow(1 - DistanceBetweenGenomes.phredToProbability(5), 2)
    result.keySet should equal(Set("TC", "CG", "GA", "AT"))
    result("TC") should equal(weight * 2)
    result("CG") should equal(weight * 2)
    result("GA") should equal(weight * 2)
    result("AT") should equal(weight)
  }

  test("extract kmers: simple, multiple reads") {
    val result = DistanceBetweenGenomes.extractKmers(
      kValues = Seq(2),
      reads = Seq(
        TestUtil.makeUnmappedRead("TCGATCGA", qualityScores = Some(Seq(5, 5, 5, 5, 5, 5, 5, 5))),
        TestUtil.makeUnmappedRead("TCGATCGA", qualityScores = Some(Seq(5, 5, 5, 5, 5, 5, 5, 5))))
    ).map(pair => (pair._1._1, pair._2)).toMap
    println(result.mkString(", "))
    val weight = math.pow(1 - DistanceBetweenGenomes.phredToProbability(5), 2) * 2 // two reads
    result.keySet should equal(Set("TC", "CG", "GA", "AT"))
    result("TC") should equal(weight * 2)
    result("CG") should equal(weight * 2)
    result("GA") should equal(weight * 2)
    result("AT") should equal(weight)
  }

  test("extract kmers: has an N") {
    val result = DistanceBetweenGenomes.extractKmers(
      kValues = Seq(2),
      reads = Seq(
        TestUtil.makeUnmappedRead("TCGNTCGA", qualityScores = Some(Seq(5, 5, 5, 5, 5, 5, 5, 5))))
    ).map(pair => (pair._1._1, pair._2)).toMap
    println(result.mkString(", "))
    val weight = math.pow(1 - DistanceBetweenGenomes.phredToProbability(5), 2)
    result.keySet should equal(Set("TC", "CG", "GA"))
    result("TC") should equal(weight * 2)
    result("CG") should equal(weight * 2)
    result("GA") should equal(weight)
  }

  test("extract kmers: 0 quality score") {
    val result = DistanceBetweenGenomes.extractKmers(
      kValues = Seq(2),
      reads = Seq(
        TestUtil.makeUnmappedRead("TCGATCGA", qualityScores = Some(Seq(5, 5, 5, 0, 5, 5, 5, 5))))
    ).map(pair => (pair._1._1, pair._2)).toMap
    println(result.mkString(", "))
    val weight = math.pow(1 - DistanceBetweenGenomes.phredToProbability(5), 2)
    result.keySet should equal(Set("TC", "CG", "GA"))
    result("TC") should equal(weight * 2)
    result("CG") should equal(weight * 2)
    result("GA") should equal(weight)
  }

  test("extract kmers: two 0 quality scores") {
    val result = DistanceBetweenGenomes.extractKmers(
      kValues = Seq(2),
      reads = Seq(
        TestUtil.makeUnmappedRead("TCGATCGA", qualityScores = Some(Seq(5, 5, 5, 0, 0, 5, 5, 5))))
    ).map(pair => (pair._1._1, pair._2)).toMap
    println(result.mkString(", "))
    val weight = math.pow(1 - DistanceBetweenGenomes.phredToProbability(5), 2)
    result.keySet should equal(Set("TC", "CG", "GA"))
    result("TC") should equal(weight)
    result("CG") should equal(weight * 2)
    result("GA") should equal(weight)
  }

}
