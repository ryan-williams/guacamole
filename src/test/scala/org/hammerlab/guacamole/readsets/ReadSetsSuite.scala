package org.hammerlab.guacamole.readsets

import com.esotericsoftware.kryo.Kryo
import org.bdgenomics.adam.models.{SequenceDictionary, SequenceRecord}
import org.hammerlab.guacamole.reads.{MappedRead, Read}
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.reference.ReferenceBroadcast.MapBackedReferenceSequence
import org.hammerlab.guacamole.util.{Bases, GuacFunSuite, KryoTestRegistrar, TestUtil}

import scala.collection.mutable

class ReadSetsSuiteRegistrar extends KryoTestRegistrar {
  override def registerTestClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Array[IndexedSeq[_]]])
  }
}

class ReadSetsSuite extends GuacFunSuite with Util {

  override def registrar: String = "org.hammerlab.guacamole.readsets.ReadSetsSuiteRegistrar"

  val contigLengths: ContigLengths =
    Map(
      "chr1" -> 200,
      "chr2" -> 300,
      "chr5" -> 400
    )

  val sequenceDictionary =
    SequenceDictionary(
      (for {
        (c, l) <- contigLengths.toSeq
      } yield
        SequenceRecord(c, l)
      ): _*
    )

  val refStrs =
    List(
      ("chr1", 100, "AACGGTTT"),
      ("chr2", 8, "TTG"),
      ("chr2", 102, "AACC"),
      ("chr5", 90, "G")
    )

  val refsMap = mutable.Map[String, mutable.Map[Int, Byte]]()

  for {
    (contig, pos, sequence) <- refStrs
    (base, idx) <- sequence.zipWithIndex
  } {
    refsMap
      .getOrElseUpdate(
        contig,
        mutable.Map[Int, Byte]()
      )
      .getOrElseUpdate(
        pos + idx,
        base.toByte
      )
  }

  lazy val reference: ReferenceBroadcast =
    ReferenceBroadcast(
      (for {
        (contig, basesMap) <- refsMap
        contigLength = contigLengths(contig)
        basesMapBroadcast = sc.broadcast(basesMap.toMap)
      } yield
        contig -> MapBackedReferenceSequence(contigLength.toInt, basesMapBroadcast)
      ).toMap
    )

  type TestRead = (String, Int, Int, Int)
  type TestReads = Seq[TestRead]
  type TestPos = (String, Int)
  type ExpectedReadIdx = (Int, Int)
  type ExpectedReadIdxs = Iterable[ExpectedReadIdx]
  type ExpectedReadIdxs3 = (ExpectedReadIdxs, ExpectedReadIdxs, ExpectedReadIdxs)

  type ExpectedPosReads = (TestPos, ExpectedReadIdxs)

  type ExpectedPosReadsPerSample = (TestPos, PerSample[ExpectedReadIdxs])
  type ExpectedPosPerSample = (TestPos, ExpectedReadIdxs3)

  def makeReadSets(reads: TestReads, numPartitions: Int): (ReadSets, Seq[Read]) = {
    val (readsets, allReads) = makeReadSets(Vector(reads), numPartitions)
    (readsets, allReads(0))
  }

  def makeReadSets(reads: PerSample[TestReads], numPartitions: Int): (ReadSets, PerSample[Seq[Read]]) = {
    val (readsRDDs, allReads) =
      (for {
        (sampleReads, sampleId) <- reads.zipWithIndex
      } yield {
        val mappedReads =
          for {
            (contig, start, end, num) <- sampleReads
            referenceContig = reference.getContig(contig)
            sequence = Bases.basesToString(referenceContig.slice(start, end))
            i <- 0 until num
          } yield
            TestUtil.makeRead(
              sequence,
              cigar = (end - start).toString + "M",
              start = start,
              chr = contig,
              sampleId = sampleId
            ): Read

        val rdd = sc.parallelize(mappedReads, numPartitions)

        ReadsRDD(rdd, "test", contigLengths) -> mappedReads
      }).unzip

    (ReadSets(readsRDDs, sequenceDictionary), allReads.toVector)
  }

  def testPileups(halfWindowSize: Int,
                  maxRegionsPerPartition: Int,
                  numPartitions: Iterable[Int],
                  reads: TestReads,
                  expected: Iterable[ExpectedPosReads]): Unit =
    for {
      numPartitions <- numPartitions
    } {
      testPileups(halfWindowSize, maxRegionsPerPartition, numPartitions, reads, expected)
    }

  def testPileups(halfWindowSize: Int,
                  maxRegionsPerPartition: Int,
                  numPartitions: Int,
                  reads: TestReads,
                  expected: Iterable[ExpectedPosReads]): Unit = {

    val (readsets, allReads) = makeReadSets(reads, numPartitions)

    val pileups =
      readsets
        .pileups(halfWindowSize, maxRegionsPerPartition, reference)
        .map(pileup =>
          (
            pileup.referenceName,
            pileup.locus,
            pileup.elements.map(e =>
              (
                e.read,
                e.locus,
                e.readPosition
              )
            ).toArray.sortBy(-_._3).toList
          )
        )
        .collect()

    withClue(s"numPartitions: $numPartitions\n") {
      pileups.length should be(expected.size)

      for {
        (pileup, ((contig, locus), readIndices)) <- pileups.zip(expected)
      } {

        val expectedReads =
          for {
            (readIdx, readPosition) <- readIndices.toList
          } yield {
            (allReads(readIdx), locus, readPosition)
          }

        pileup should be(
          (
            contig,
            locus,
            expectedReads
          )
        )
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
        ("chr1",  99) -> List(),
        ("chr1", 100) -> List((0, 0)),
        ("chr1", 101) -> List((0, 1), (1, 0)),
        ("chr1", 102) -> List((0, 2), (1, 1)),
        ("chr1", 103) -> List((0, 3), (1, 2)),
        ("chr1", 104) -> List((0, 4), (1, 3)),
        ("chr1", 105) -> List((1, 4)),
        ("chr1", 106) -> List(),
        ("chr2",   7) -> List(),
        ("chr2",   8) -> List((2, 0)),
        ("chr2",   9) -> List((3, 0)),
        ("chr2",  10) -> List((3, 1)),
        ("chr2",  11) -> List(),
        ("chr2", 101) -> List(),
        ("chr2", 102) -> List((4, 0)),
        ("chr2", 103) -> ((4, 1) :: (5 until 15).toList.map(i => (i, 0))),
        ("chr2", 104) -> ((4, 2) :: (5 until 15).toList.map(i => (i, 1))),
        ("chr2", 105) -> (5 until 15).toList.map(i => (i, 2)),
        ("chr2", 106) -> List(),
        ("chr5",  89) -> List(),
        ("chr5",  90) -> (15 until 25).toList.map(i => (i, 0)),
        ("chr5",  91) -> List()
      )

    test("pileups: small") {
      testPileups(
        halfWindowSize = 1,
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
        halfWindowSize = 1,
        maxRegionsPerPartition = 11,
        numPartitions = 1 to 4,
        reads,
        pileups
      )
    }

    test("pileups: drop deepest loci") {
      testPileups(
        halfWindowSize = 1,
        maxRegionsPerPartition = 10,
        numPartitions = 1 to 4,
        reads = reads,
        expected = pileups.filter {
          // Loci [102,105] have ≥ 10 depth and get dropped here.
          case (("chr2", locus), _) if 102 <= locus && locus <= 105 => false
          case _ => true
        }
      )
    }
  }


  def testPerSamplePileups(halfWindowSize: Int,
                           maxRegionsPerPartition: Int,
                           numPartitions: Int,
                           reads: PerSample[TestReads],
                           expected: Iterable[ExpectedPosPerSample]): Unit = {
    val (readsets, allReads) = makeReadSets(reads, numPartitions)

    val pileups: Array[PerSample[(String, Long, List[(MappedRead, Long, Int)])]] =
      readsets
        .perSamplePileups(halfWindowSize, maxRegionsPerPartition, reference)
        .map(
          _.map(pileup =>
            (
              pileup.referenceName,
              pileup.locus,
              pileup.elements.map(e =>
                (
                  e.read,
                  e.locus,
                  e.readPosition
                )
              ).toArray.sortBy(-_._3).toList
            )
          )
        )
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
            (((pileup, readIndices), mappedReads), sampleId) <- pileups.zip(reads).zip(allReads).zipWithIndex
          } {

            withClue(s"sample: $sampleId\n") {
              val expectedReads =
                for {
                  (readIdx, readPosition) <- readIndices.toList
                } yield
                  (mappedReads(readIdx), locus, readPosition)

              pileup should be(
                (
                  contig,
                  locus,
                  expectedReads
                )
              )
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
        ("chr1", 106, 107, 1)/*,
        ("chr2",   8,   9, 5),
        ("chr2",  20,  22, 3)*/
      )

    val sample2Reads =
      List(
//        ("chr2",   8,   9, 5),
//        ("chr2",  30,  32, 3)
      )

    val sample3Reads =
      List(
        ("chr1", 101, 102, 1),
        ("chr1", 102, 103, 5),
        ("chr1", 104, 105, 3)/*,
        ("chr2",   8,   9, 5),
        ("chr2",  40,  42, 3)*/
      )

    val none: ExpectedReadIdxs = Nil

    val pileups: Iterable[ExpectedPosPerSample] =
      List[ExpectedPosPerSample](
        "chr1" ->  98 -> (none, none, none),
        "chr1" ->  99 -> (List(), List(), List()),
        "chr1" -> 100 -> (List(0 -> 0, 1 -> 0), List(), List()),
        "chr1" -> 101 -> (List(0 -> 1, 1 -> 1), List(), List(0 -> 0)),
        "chr1" -> 102 -> (List(0 -> 2, 1 -> 2, 2 -> 0, 3 -> 0), List(), List(1 -> 0, 2 -> 0, 3 -> 0, 4 -> 0, 5 -> 0)),
        "chr1" -> 103 -> (List(2 -> 1, 3 -> 1), List(), List()),
        "chr1" -> 104 -> (List(), List(), List(6 -> 0, 7 -> 0, 8 -> 0)),
        "chr1" -> 105 -> (List(), List(), List()),
        "chr1" -> 106 -> (List(4 -> 0), List(), List()),
        "chr1" -> 107 -> (List(), List(), List()),
        "chr1" -> 108 -> (List(), List(), List())/*,
        "chr2" ->   6 -> (List(), List(), List()),
        "chr2" ->   7 -> (List(), List(), List()),
        "chr2" ->   8 -> (List(), List(), List()),
        "chr2" ->   9 -> (List(), List(), List()),
        "chr2" ->  18 -> (List(), List(), List()),
        "chr2" ->  19 -> (List(), List(), List()),
        "chr2" ->  20 -> (List(), List(), List()),
        "chr2" ->  21 -> (List(), List(), List()),
        "chr2" ->  22 -> (List(), List(), List()),
        "chr2" ->  23 -> (List(), List(), List()),
        "chr2" ->  28 -> (List(), List(), List()),
        "chr2" ->  29 -> (List(), List(), List()),
        "chr2" ->  30 -> (List(), List(), List()),
        "chr2" ->  31 -> (List(), List(), List()),
        "chr2" ->  32 -> (List(), List(), List()),
        "chr2" ->  33 -> (List(), List(), List()),
        "chr2" ->  38 -> (List(), List(), List()),
        "chr2" ->  39 -> (List(), List(), List()),
        "chr2" ->  40 -> (List(), List(), List()),
        "chr2" ->  41 -> (List(), List(), List()),
        "chr2" ->  42 -> (List(), List(), List()),
        "chr2" ->  43 -> (List(), List(), List())*/
      )

    test("per-sample pileups") {
      testPerSamplePileups(
        halfWindowSize = 2,
        maxRegionsPerPartition = 20,
        numPartitions = 1,
        reads = Vector(sample1Reads, sample2Reads, sample3Reads),
        expected = pileups
      )
    }
  }
}
