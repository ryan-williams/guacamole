package org.hammerlab.guacamole.readsets

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.loci.Coverage.PositionCoverage
import org.hammerlab.magic.rdd.CmpStats
import org.hammerlab.guacamole.readsets.RegionRDD._
import org.hammerlab.guacamole.util.{GuacFunSuite, KryoTestRegistrar}
import org.scalatest.Matchers
import org.hammerlab.magic.rdd.EqualsRDD._

import scala.collection.SortedMap

class RegionRDDSuiteRegistrar extends KryoTestRegistrar {
  override def registerTestClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Array[TestRegion]])
    kryo.register(classOf[TestRegion])
    kryo.register(classOf[CmpStats])
  }
}

class RegionRDDSuite extends GuacFunSuite with Matchers {

  override def registrar = "org.hammerlab.guacamole.readsets.RegionRDDSuiteRegistrar"

  def makeReads(reads: (String, Int, Int, Int)*): Seq[TestRegion] =
    (for {
      (contig, start, end, num) <- reads
      i <- 0 until num
    } yield
      TestRegion(contig, start, end)
    )

  def testRDD(rdd: RDD[PositionCoverage], expected: List[(String, (Int, Int, Int))]): Unit = {
    val actual = rdd.collect()
    val actualStrs =
      for {
        (pos, Coverage(depth, starts, ends)) <- actual
      } yield {
        pos.toString -> (depth, starts, ends)
      }

    val actualMap = SortedMap(actualStrs: _*)
    val expectedMap = SortedMap(expected: _*)

    val extraElems = actualMap.filterKeys(!expectedMap.contains(_))
    val missingElems = expectedMap.filterKeys(!actualMap.contains(_))

    val diffElems =
      for {
        (k, ev) <- expectedMap
        av <- actualMap.get(k)
        if ev != av
      } yield
        k -> (av, ev)

    withClue("differing loci:") { diffElems should be(Map()) }
    withClue("found extra loci:") { extraElems should be(Map()) }
    withClue("missing loci:") { missingElems should be(Map()) }

    withClue("loci out of order:") {
      actual.map(_._1).sortBy(x => x) should be(actual.map(_._1))
    }
  }

  test("simple") {
    val reads =
      makeReads(
        ("chr1", 100, 105,  1),
        ("chr1", 101, 106,  1),
        ("chr2",   8,   9,  1),
        ("chr2",   9,  11,  1),
        ("chr2", 102, 105,  1),
        ("chr2", 103, 106, 10),
        ("chr5",  90,  91, 10)
      )

    implicit val contigLengthsBroadcast: Broadcast[ContigLengths] =
      sc.broadcast(Map("chr1" -> 1000, "chr2" -> 1000, "chr5" -> 1000))

    val readsRDD = sc.parallelize(reads, 1)
    val rdd = readsRDD.coverage(0)

    val expected =
      List(
        "chr1:100" -> ( 1,  1,  0),
        "chr1:101" -> ( 2,  1,  0),
        "chr1:102" -> ( 2,  0,  0),
        "chr1:103" -> ( 2,  0,  0),
        "chr1:104" -> ( 2,  0,  0),
        "chr1:105" -> ( 1,  0,  1),
        "chr1:106" -> ( 0,  0,  1),
          "chr2:8" -> ( 1,  1,  0),
          "chr2:9" -> ( 1,  1,  1),
         "chr2:10" -> ( 1,  0,  0),
         "chr2:11" -> ( 0,  0,  1),
        "chr2:102" -> ( 1,  1,  0),
        "chr2:103" -> (11, 10,  0),
        "chr2:104" -> (11,  0,  0),
        "chr2:105" -> (10,  0,  1),
        "chr2:106" -> ( 0,  0, 10),
         "chr5:90" -> (10, 10,  0),
         "chr5:91" -> ( 0,  0, 10)
      )

    val shuffled = readsRDD.shuffleCoverage(0)

    testRDD(rdd, expected)
    testRDD(shuffled, expected)

    rdd.compare(shuffled) should be(CmpStats(18))
  }
}
