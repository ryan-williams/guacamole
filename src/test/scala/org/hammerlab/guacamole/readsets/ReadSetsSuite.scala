package org.hammerlab.guacamole.readsets

import org.bdgenomics.adam.models.{SequenceDictionary, SequenceRecord}
import org.hammerlab.guacamole.reads.Read
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.reference.ReferenceBroadcast.MapBackedReferenceSequence
import org.hammerlab.guacamole.util.{Bases, GuacFunSuite, TestUtil}

import scala.collection.{SortedMap, mutable}

class ReadSetsSuite extends GuacFunSuite with Util {

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

  def testPileups(halfWindowSize: Int,
                  maxRegionsPerPartition: Int,
                  numPartitions: Iterable[Int],
                  reads: Seq[(String, Int, Int, Int)],
                  refs: Seq[(String, Int, String)],
                  expected: Iterable[((String, Int), Iterable[(Int, Int)])]): Unit =
    testPileups(halfWindowSize, maxRegionsPerPartition, numPartitions, reads: _*)(refs: _*)(expected.toSeq: _*)

  def testPileups(halfWindowSize: Int,
                  maxRegionsPerPartition: Int,
                  numPartitions: Int,
                  reads: Seq[(String, Int, Int, Int)],
                  refs: Seq[(String, Int, String)],
                  expected: Iterable[((String, Int), Iterable[(Int, Int)])]): Unit =
    testPileups(halfWindowSize, maxRegionsPerPartition, numPartitions, reads: _*)(refs: _*)(expected.toSeq: _*)

  def testPileups(
    halfWindowSize: Int,
    maxRegionsPerPartition: Int,
    numPartitions: Iterable[Int],
    readStrs: (String, Int, Int, Int)*
  )(
    refs: (String, Int, String)*
  )(
    expected: ((String, Int), Iterable[(Int, Int)])*
  ): Unit =
    for {
      numPartitions <- numPartitions
    } {
      testPileups(halfWindowSize, maxRegionsPerPartition, numPartitions, readStrs: _*)(refs: _*)(expected: _*)
    }

  def testPileups(
    halfWindowSize: Int,
    maxRegionsPerPartition: Int,
    numPartitions: Int,
    readStrs: (String, Int, Int, Int)*
  )(
    refs: (String, Int, String)*
  )(
    expected: ((String, Int), Iterable[(Int, Int)])*
  ): Unit = {

    val refsMap = mutable.Map[String, mutable.Map[Int, Byte]]()

    for {
      (contig, pos, sequence) <- refs
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

    val reference =
      ReferenceBroadcast(
        (for {
          (contig, basesMap) <- refsMap
          contigLength = contigLengths(contig)
          basesMapBroadcast = sc.broadcast(basesMap.toMap)
        } yield
          contig -> MapBackedReferenceSequence(contigLength.toInt, basesMapBroadcast)
        ).toMap
    )

    val reads =
      for {
        (contig, start, end, num) <- readStrs
        referenceContig = reference.getContig(contig)
        sequence = Bases.basesToString(referenceContig.slice(start, end))
        i <- 0 until num
      } yield
        TestUtil.makeRead(sequence, cigar = (end - start).toString + "M", start = start, chr = contig): Read

    val rdd = sc.parallelize(reads, numPartitions)

    val readsRDD = ReadsRDD(rdd, "test", contigLengths)

    val readsets = ReadSets(Vector(readsRDD), sequenceDictionary)

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
            (reads(readIdx), locus, readPosition)
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

  val readStrs =
    List(
      ("chr1", 100, 105,  1),
      ("chr1", 101, 106,  1),
      ("chr2",   8,   9,  1),
      ("chr2",   9,  11,  1),
      ("chr2", 102, 105,  1),
      ("chr2", 103, 106, 10),
      ("chr5",  90,  91, 10)
    )

  val refStrs =
    List(
      ("chr1", 100, "AACGGT"),
      ("chr2",   8, "TTG"),
      ("chr2", 102, "AACC"),
      ("chr5",  90, "G")
    )

  val pileups =
    SortedMap(
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

  test("pileups") {
    testPileups(
      halfWindowSize = 1,
      maxRegionsPerPartition = 10,
      numPartitions = 1 to 2,
      readStrs.slice(0, 2),
      refStrs.slice(0, 1),
      pileups.filter {
        case (("chr1", _), _) => true
        case _ => false
      }
    )
  }

  test("pileups 2") {
    testPileups(
      halfWindowSize = 1,
      maxRegionsPerPartition = 11,
      numPartitions = 1 to 4,
      readStrs,
      refStrs,
      pileups.toSeq
    )
  }

  test("pileups 3") {
    testPileups(
      halfWindowSize = 1,
      maxRegionsPerPartition = 10,
      numPartitions = 1 to 4,
      reads = readStrs,
      refs = refStrs,
      expected = pileups.filter {
        // Loci [102,105] have ≥ 10 depth.
        case (("chr2", locus), _) if 102 <= locus && locus <= 105 => false
        case _ => true
      }
    )
  }

}
