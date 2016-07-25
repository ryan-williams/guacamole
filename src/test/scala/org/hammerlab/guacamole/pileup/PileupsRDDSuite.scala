package org.hammerlab.guacamole.pileup

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{SequenceDictionary, SequenceRecord}
import org.hammerlab.guacamole.pileup.PileupsRDD._
import org.hammerlab.guacamole.pileup.PileupsRDDUtil.{TestPileup, simplifyPileup}
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.readsets.ReadSetsUtil.TestReads
import org.hammerlab.guacamole.readsets.rdd.PartitionedRegionsUtil
import org.hammerlab.guacamole.readsets.{ContigLengths, PerSample, ReadSetsUtil}
import org.hammerlab.guacamole.reference.{ContigName, Locus, ReferenceUtil}
import org.hammerlab.guacamole.util.{GuacFunSuite, KryoTestRegistrar}
import org.scalatest.Matchers

import scala.collection.mutable

class PileupsRDDSuiteRegistrar extends KryoTestRegistrar {
  override def registerTestClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Array[IndexedSeq[_]]])
  }
}

class PileupsRDDSuite
  extends GuacFunSuite
    with PartitionedRegionsUtil
    with ReferenceUtil
    with ReadSetsUtil {

  type TestPos = (String, Int)

  type ExpectedReadIdx = Int
  type ExpectedReadIdxs = Iterable[ExpectedReadIdx]
  type ExpectedReadIdxs3 = (ExpectedReadIdxs, ExpectedReadIdxs, ExpectedReadIdxs)

  type ExpectedReadsStr = String
  type ExpectedReadsStr3 = (ExpectedReadsStr, ExpectedReadsStr, ExpectedReadsStr)

  type ExpectedPosReads = (TestPos, ExpectedReadsStr)

  type ExpectedPosPerSample = (TestPos, ExpectedReadsStr3)

  override def registrar: String = "org.hammerlab.guacamole.pileup.PileupsRDDSuiteRegistrar"

  // Used to create PartitionedRegions.
  implicit val contigLengths: ContigLengths =
    makeContigLengths(
      "chr1" -> 200,
      "chr2" -> 300,
      "chr5" -> 400
    )

  // Used to create ReadSets.
  implicit val sequenceDictionary =
    SequenceDictionary(
      (for {
        (c, l) <- contigLengths.toSeq
      } yield
        SequenceRecord(c, l)
      ): _*
    )

  // Used to create ReadSets.
  implicit lazy val reference =
    makeReference(
      contigLengths,
      ("chr1", 100, "AACGGTTT"),
      ("chr2", 8, "TTG"),
      ("chr2", 102, "AACC"),
      ("chr5", 90, "G")
    )

  def checkPileup(contigName: ContigName,
                  locus: Int,
                  reads: Seq[MappedRead],
                  readsStr: ExpectedReadsStr,
                  pileup: TestPileup): Unit = {
    pileup should be(
      (
        contigName,
        locus,
        readsStr
      )
    )
  }

  def testPileups(maxRegionsPerPartition: Int,
                  numPartitions: Iterable[Int],
                  reads: TestReads,
                  expected: Iterable[ExpectedPosReads]): Unit =
    for {
      numPartitions <- numPartitions
    } {
      testPileups(maxRegionsPerPartition, numPartitions, reads, expected)
    }

  def testPileups(maxRegionsPerPartition: Int,
                  numPartitions: Int,
                  reads: TestReads,
                  expected: Iterable[ExpectedPosReads]): Unit = {

    val (readsets, mappedReads) = makeReadSets(reads, numPartitions)

    val partitionedReads =
      makePartitionedReads(
        readsets,
        halfWindowSize = 0,
        maxRegionsPerPartition,
        numPartitions
      )

    val pileups =
      partitionedReads
        .pileups(reference)
        .map(simplifyPileup)
        .collect()

    withClue(s"numPartitions: $numPartitions\n") {
      pileups.length should be(expected.size)

      for {
        (pileup, ((contig, locus), readsStr)) <- pileups.zip(expected)
      } {
        checkPileup(contig, locus, mappedReads, readsStr, pileup)
      }
    }
  }


  {
    // Some tests of the all-sample Pileup-iteration logic.
    val reads =
      List(
        ("chr1", 100, 105, 1),
        ("chr1", 101, 106, 1),
        ("chr2", 8, 9, 1),
        ("chr2", 9, 11, 1),
        ("chr2", 102, 105, 1),
        ("chr2", 103, 106, 10),
        ("chr5", 90, 91, 10)
      )

    val pileups =
      List(
        ("chr1", 100) -> "[100,105)",
        ("chr1", 101) -> "[100,105), [101,106)",
        ("chr1", 102) -> "[100,105), [101,106)",
        ("chr1", 103) -> "[100,105), [101,106)",
        ("chr1", 104) -> "[100,105), [101,106)",
        ("chr1", 105) -> "[101,106)",
        ("chr2",   8) -> "[8,9)",
        ("chr2",   9) -> "[9,11)",
        ("chr2",  10) -> "[9,11)",
        ("chr2", 102) -> "[102,105)",
        ("chr2", 103) -> "[102,105), [103,106)*10",
        ("chr2", 104) -> "[102,105), [103,106)*10",
        ("chr2", 105) -> "[103,106)*10",
        ("chr5",  90) -> "[90,91)*10"
      )

    test("pileups: small") {
      testPileups(
        maxRegionsPerPartition = 10,
        numPartitions = 1 to 2,
        reads.slice(0, 2),
        pileups.filter {
          case (("chr1", _), _) => true
          case _ => false
        }
      )
    }

    test("pileups: large") {
      testPileups(
        maxRegionsPerPartition = 11,
        numPartitions = 1 to 4,
        reads,
        pileups
      )
    }

    test("pileups: drop deepest loci") {
      testPileups(
        maxRegionsPerPartition = 10,
        numPartitions = 1 to 4,
        reads = reads,
        expected = pileups.filter {
          // Loci [103,104] have ≥ 10 depth and get dropped here.
          case (("chr2", locus), _) if 103 <= locus && locus <= 104 => false
          case _ => true
        }
      )
    }
  }

  def testPerSamplePileups(maxRegionsPerPartition: Int,
                           numPartitions: Int,
                           perSampleReads: PerSample[TestReads],
                           expected: Iterable[ExpectedPosPerSample]): Unit = {
    val (readsets, mappedReads) = makeReadSets(perSampleReads, numPartitions)

    val partitionedReads = makePartitionedReads(readsets, halfWindowSize = 0, maxRegionsPerPartition, numPartitions)

    val numSamples = perSampleReads.length

    val pileups: Array[PerSample[(ContigName, Locus, ExpectedReadsStr)]] =
      partitionedReads
        .perSamplePileups(numSamples, reference)
        .map(_.map(simplifyPileup))
        .collect()

    withClue(s"numPartitions: $numPartitions\n") {
      pileups.length should be(expected.size)

      for {
        (pileups, ((contig, locus), (reads1, reads2, reads3))) <- pileups.zip(expected)
        reads = Vector(reads1, reads2, reads3)
      } {
        pileups.length should be(3)

        withClue(s"$contig:$locus:\n") {
          for {
            (((pileup, readsStr), sampleReads), sampleId) <- pileups.zip(reads).zip(mappedReads).zipWithIndex
          } {
            withClue(s"sample: $sampleId\n") {
              checkPileup(contig, locus, sampleReads, readsStr, pileup)
            }
          }
        }
      }
    }
  }

  {
    val sample1Reads =
      List(
        ("chr1", 100, 103, 2),
        ("chr1", 102, 104, 2),
        ("chr1", 106, 107, 1),
        ("chr2",   8,   9, 5),
        ("chr2",  20,  22, 3)
      )

    val sample2Reads =
      List(
        ("chr2",   8,   9, 5),
        ("chr2",  30,  32, 3)
      )

    val sample3Reads =
      List(
        ("chr1", 101, 102, 1),
        ("chr1", 102, 103, 5),
        ("chr1", 104, 105, 3),
        ("chr2",   8,   9, 5),
        ("chr2",  40,  42, 3)
      )

    val pileups: Iterable[ExpectedPosPerSample] =
      List[ExpectedPosPerSample](
        "chr1" -> 100 -> ("[100,103)*2", "", ""),
        "chr1" -> 101 -> ("[100,103)*2", "", "[101,102)"),
        "chr1" -> 102 -> ("[100,103)*2, [102,104)*2", "", "[102,103)*5"),
        "chr1" -> 103 -> ("[102,104)*2", "", ""),
        "chr1" -> 104 -> ("", "", "[104,105)*3"),
        "chr1" -> 106 -> ("[106,107)", "", ""),
        "chr2" ->   8 -> ("[8,9)*5", "[8,9)*5", "[8,9)*5"),
        "chr2" ->  20 -> ("[20,22)*3", "", ""),
        "chr2" ->  21 -> ("[20,22)*3", "", ""),
        "chr2" ->  30 -> ("", "[30,32)*3", ""),
        "chr2" ->  31 -> ("", "[30,32)*3", ""),
        "chr2" ->  40 -> ("", "", "[40,42)*3"),
        "chr2" ->  41 -> ("", "", "[40,42)*3")
      )

    test("per-sample pileups") {
      testPerSamplePileups(
        maxRegionsPerPartition = 20,
        numPartitions = 1,
        perSampleReads = Vector(sample1Reads, sample2Reads, sample3Reads),
        expected = pileups
      )
    }
  }
}

object PileupsRDDUtil {
  type TestPileup = (ContigName, Locus, String)

  /**
   * This is passed to [[RDD.map]] above, so it must live outside of [[PileupsRDDSuite]], which is not serializable (due
   * to its [[Matchers.assertionsHelper]] memeber).
   * @param pileup
   * @return
   */
  def simplifyPileup(pileup: Pileup): TestPileup =
    (
      pileup.contigName,
      pileup.locus,
      {
        val reads = mutable.Map[(Long, Long), Int]()
        for {
          e <- pileup.elements
          read = e.read
          contig = read.contigName
          start = read.start
          end = read.end
        } {
          reads((start, end)) = reads.getOrElseUpdate((start, end), 0) + 1
        }

        (for {
          ((start, end), count) <- reads.toArray.sortBy(_._1._2)
        } yield
          s"[$start,$end)${if (count > 1) s"*$count" else ""}"
          ).mkString(", ")
      }
    )
}
