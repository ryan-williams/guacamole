package org.hammerlab.guacamole.pileup

import org.hammerlab.genomics.bases.Base.{ A, C, G, T }
import org.hammerlab.genomics.reads.{ MappedRead, ReadsUtil }
import org.hammerlab.genomics.reference.Locus
import org.hammerlab.guacamole.reference.ReferenceUtil
import org.hammerlab.guacamole.util.GuacFunSuite
import org.hammerlab.guacamole.variants.Allele
import org.scalatest.prop.TableDrivenPropertyChecks

class PileupSuite
  extends GuacFunSuite
    with TableDrivenPropertyChecks
    with ReadsUtil
    with Util
    with ReferenceUtil {

  // This must only be accessed from inside a spark test where SparkContext has been initialized
  override lazy val reference =
    makeReference(
      sc,
      ("chr1", 0, "CTCGATCGACG"),
      ("1", 229538778, "A" * 191135),
      ("artificial", 0, "A" * 34 + "G" * 10 + "A" * 5 + "G" * 15 + "A" * 15 + "ACGT" * 10),
      ("chr2", 0, "AATTG"),
      ("chr3", 0, "AAATTT"),
      ("chr4", 0, "AATTGCAATTG")
    )

  def testAdamRecords = loadReadsRDD(sc, "different_start_reads.sam").mappedReads.collect()

  def pileupElementFromRead(read: MappedRead, locus: Locus): PileupElement = {
    PileupElement(read, locus, reference.getContig(read.contigName))
  }

  test("create pileup from long insert reads") {
    val reads =
      makeReads(
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("TCGACCCTCGA", "4M3I4M", 1)
      )

    val firstPileup = makePileup(reads, "chr1", 1)
    firstPileup.elements.forall(_.isMatch) should === (true)
    firstPileup.elements.forall(_.qualityScore == 31) should === (true)

    val insertPileup = makePileup(reads, "chr1", 4)
    insertPileup.elements.exists(_.isInsertion) should === (true)
    insertPileup.elements.forall(_.qualityScore == 31) should === (true)

    insertPileup.elements(0).alignment should === (Match(A, 31.toByte))
    insertPileup.elements(1).alignment should === (Match(A, 31.toByte))
    insertPileup.elements(2).alignment should === (Insertion("ACCC", Seq(31, 31, 31, 31).map(_.toByte)))
  }

  test("create pileup from long insert reads; different qualities in insertion") {
    val reads =
      Seq(
        makeRead("TCGATCGA", "8M", 1, "chr1", Seq(10, 15, 20, 25, 10, 15, 20, 25)),
        makeRead("TCGATCGA", "8M", 1, "chr1", Seq(10, 15, 20, 25, 10, 15, 20, 25)),
        makeRead("TCGACCCTCGA", "4M3I4M", 1, "chr1", Seq(10, 15, 20, 25, 5, 5, 5, 10, 15, 20, 25))
      )

    val insertPileup = makePileup(reads, "chr1", 4)
    insertPileup.elements.exists(_.isInsertion) should === (true)
    insertPileup.elements.exists(_.qualityScore == 5) should === (true)

    insertPileup.elements.foreach(
      _.alignment match {
        case Match(_, quality)       => quality should === (25)
        case Insertion(_, qualities) => qualities should === (Seq[Byte](25, 5, 5, 5))
        case a                       => assert(false, s"Unexpected Alignment: $a")
      })
  }

  test("create pileup from long insert reads, right after insertion") {
    val reads =
      Seq(
        makeRead("TCGATCGA", "8M", 1, "chr1", Seq(10, 15, 20, 25, 10, 15, 20, 25)),
        makeRead("TCGATCGA", "8M", 1, "chr1", Seq(10, 15, 20, 25, 10, 15, 20, 25)),
        makeRead("TCGACCCTCGA", "4M3I4M", 1, "chr1", Seq(10, 15, 20, 25, 5, 5, 5, 10, 15, 20, 25))
      )

    val pastInsertPileup = makePileup(reads, "chr1", 5)
    pastInsertPileup.elements.foreach(_.isMatch should === (true))
    pastInsertPileup.elements.foreach(_.qualityScore should === (10))
  }

  test("create pileup from long insert reads; after insertion") {
    val reads =
      makeReads(
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("TCGACCCTCGA", "4M3I4M", 1)
      )

    val lastPileup = makePileup(reads, "chr1", 7)
    lastPileup.elements.foreach(e => e.sequencedBases should ===("G"))
    lastPileup.elements.forall(_.isMatch) should === (true)
  }

  test("create pileup from long insert reads; end of read") {

    val reads =
      Seq(
        makeRead("TCGATCGA", "8M", 1, "chr1", Seq(10, 15, 20, 25, 10, 15, 20, 25)),
        makeRead("TCGATCGA", "8M", 1, "chr1", Seq(10, 15, 20, 25, 10, 15, 20, 25)),
        makeRead("TCGACCCTCGA", "4M3I4M", 1, "chr1", Seq(10, 15, 20, 25, 5, 5, 5, 10, 15, 20, 25))
      )

    val lastPileup = makePileup(reads, "chr1", 8)
    lastPileup.elements.foreach(e => e.sequencedBases should ===("A"))
    lastPileup.elements.forall(_.sequencedBases.headOption.exists(_ === A)) should === (true)

    lastPileup.elements.forall(_.isMatch) should === (true)
    lastPileup.elements.forall(_.qualityScore == 25) should === (true)
  }

  test("Load pileup from SAM file") {
    val pileup = loadPileup(sc, "same_start_reads.sam", locus = 0)
    pileup.elements.length should === (10)
  }

  test("First 60 loci should have all 10 reads") {
    val pileup = loadPileup(sc, "same_start_reads.sam", locus = 0)
    for (i <- 1 to 59) {
      val nextPileup = pileup.atGreaterLocus(i, Seq.empty.iterator)
      nextPileup.elements.length should === (10)
    }
  }

  test("test pileup element creation") {
    val read = makeRead("AATTG", "5M", 0, "chr2")
    val firstElement = pileupElementFromRead(read, 0)

    firstElement.isMatch should === (true)
    firstElement.indexWithinCigarElement should === (0L)

    val secondElement = firstElement.advanceToLocus(1L)
    secondElement.isMatch should === (true)
    secondElement.indexWithinCigarElement should === (1L)

    val thirdElement = secondElement.advanceToLocus(2L)
    thirdElement.isMatch should === (true)
    thirdElement.indexWithinCigarElement should === (2L)

  }

  test("test pileup element creation with multiple cigar elements") {
    val read = makeRead("AAATTT", "3M3M", 0, "chr3")

    val secondMatch = pileupElementFromRead(read, 3)
    secondMatch.isMatch should === (true)
    secondMatch.indexWithinCigarElement should === (0L)

    val secondMatchSecondElement = pileupElementFromRead(read, 4)
    secondMatchSecondElement.isMatch should === (true)
    secondMatchSecondElement.indexWithinCigarElement should === (1L)

  }

  test("insertion at contig start includes trailing base") {
    val contigStartInsertionRead = makeRead("AAAAAACGT", "5I4M", 0, "chr1")
    val pileup = pileupElementFromRead(contigStartInsertionRead, 0)
    pileup.alignment should === (Insertion("AAAAAA", List(31, 31, 31, 31, 31, 31)))
  }

  test("pileup alignment at insertion cigar-element throws") {
    val contigStartInsertionRead = makeRead("AAAAAACGT", "5I4M", 0, "chr1")
    val pileup = PileupElement(
      read = contigStartInsertionRead,
      locus = 1,
      contigSequence = reference.getContig("chr1"),
      readPosition = 0,
      cigarElementIndex = 0,
      cigarElementLocus = 1,
      indexWithinCigarElement = 0
    )
    the[InvalidCigarElementException] thrownBy pileup.alignment
  }

  test("test pileup element creation with deletion cigar elements") {
    val read = makeRead("AATTGAATTG", "5M1D5M", 0, "chr4")
    val firstElement = pileupElementFromRead(read, 0)

    firstElement.isMatch should === (true)
    firstElement.indexWithinCigarElement should === (0L)

    val deletionElement = firstElement.advanceToLocus(4L)
    deletionElement.alignment should === (Deletion("GC", 1.toByte))
    deletionElement.isDeletion should === (true)
    deletionElement.indexWithinCigarElement should === (4L)

    val midDeletionElement = deletionElement.advanceToLocus(5L)
    midDeletionElement.isMidDeletion should === (true)
    midDeletionElement.indexWithinCigarElement should === (0L)

    val pastDeletionElement = midDeletionElement.advanceToLocus(6L)
    pastDeletionElement.isMatch should === (true)
    pastDeletionElement.indexWithinCigarElement should === (0L)

    val continuePastDeletionElement = pastDeletionElement.advanceToLocus(9L)
    continuePastDeletionElement.isMatch should === (true)
    continuePastDeletionElement.indexWithinCigarElement should === (3L)

  }

  test("Loci 10-19 deleted from half of the reads") {
    val pileup = loadPileup(sc, "same_start_reads.sam", locus = 0)
    val deletionPileup = pileup.atGreaterLocus(9, Seq.empty.iterator)

    deletionPileup.elements.map(_.alignment).count {
      case Deletion(bases, _) => {
        bases should ===("AAAAAAAAAAA")
        true
      }
      case _ => false
    } should === (5)

    for (i <- 10 to 19) {
      val nextPileup = pileup.atGreaterLocus(i, Seq.empty.iterator)
      nextPileup.elements.count(_.isMidDeletion) should === (5)
    }
  }

  test("Loci 60-69 have 5 reads") {
    val pileup = loadPileup(sc, "same_start_reads.sam", locus = 0)
    for (i <- 60 to 69) {
      val nextPileup = pileup.atGreaterLocus(i, Seq.empty.iterator)
      nextPileup.elements.length should === (5)
    }
  }

  test("Pileup.apply throws on non-overlapping reads") {
    val read = makeRead("AATTGAATTG", "5M1D5M", 1, "chr4")

    intercept[AssertionError] {
      Pileup(Seq(read), "sample", "chr4", 0, reference.getContig("chr4"))
    }

    intercept[AssertionError] {
      Pileup(Seq(read), "sample", "chr4", 12, reference.getContig("chr4"))
    }

    intercept[AssertionError] {
      Pileup(Seq(read), "sample", "chr5", 1, reference.getContig("chr4"))
    }
  }

  test("Pileup.Element basic test") {
    intercept[NullPointerException] {
      pileupElementFromRead(null, 42)
    }

    val decadentRead1 = testAdamRecords(0)

    // read1 starts at SAM:6 -> 0-based 5
    // and has CIGAR: 29M10D31M
    // so, the length is 70
    intercept[AssertionError] {
      pileupElementFromRead(decadentRead1, 0)
    }
    intercept[AssertionError] {
      pileupElementFromRead(decadentRead1, 4)
    }
    intercept[AssertionError] {
      pileupElementFromRead(decadentRead1, 5 + 70)
    }
    val at5 = pileupElementFromRead(decadentRead1, 5)
    assert(at5 != null)
    at5.sequencedBases should ===("A")

    // At the end of the read:
    assert(pileupElementFromRead(decadentRead1, 74) != null)
    intercept[AssertionError] {
      pileupElementFromRead(decadentRead1, 75)
    }

    // Just before the deletion
    val deletionPileup = pileupElementFromRead(decadentRead1, 5 + 28)
    deletionPileup.alignment should === (Deletion("AGGGGGGGGGG", 1.toByte))

    // Inside the deletion
    val at29 = pileupElementFromRead(decadentRead1, 5 + 29)
    at29.sequencedBases.size should ===(0)
    val at38 = pileupElementFromRead(decadentRead1, 5 + 38)
    at38.sequencedBases.size should ===(0)
    // Just after the deletion
    pileupElementFromRead(decadentRead1, 5 + 39).sequencedBases should ===(A)

    // advanceToLocus is a no-op on the same locus,
    // and fails in lower loci
    forAll(Table("locus", List(5, 33, 34, 43, 44, 74): _*)) { locus =>
      val elt = pileupElementFromRead(decadentRead1, locus)
      elt.advanceToLocus(locus) should ===(elt)
      intercept[AssertionError] {
        elt.advanceToLocus(locus - 1)
      }
      intercept[AssertionError] {
        elt.advanceToLocus(75)
      }
    }

    val read3Record = testAdamRecords(2) // read3
    val read3At15 = pileupElementFromRead(read3Record, 15)
    assert(read3At15 != null)
    read3At15.sequencedBases should ===("A")
    read3At15.advanceToLocus(16).sequencedBases should ===("T")
    read3At15.advanceToLocus(17).sequencedBases should ===("C")
    read3At15.advanceToLocus(16).advanceToLocus(17).sequencedBases should ===("C")
    read3At15.advanceToLocus(18).sequencedBases should ===("G")
  }

  test("Read4 has CIGAR: 10M10I10D40M. It's ACGT repeated 15 times") {
    val decadentRead4 = testAdamRecords(3)
    val read4At20 = pileupElementFromRead(decadentRead4, 20)
    assert(read4At20 != null)
    for (i <- 0 until 2) {
      read4At20.advanceToLocus(20 + i * 4 + 0).sequencedBases(0) should ===(A)
      read4At20.advanceToLocus(20 + i * 4 + 1).sequencedBases(0) should ===(C)
      read4At20.advanceToLocus(20 + i * 4 + 2).sequencedBases(0) should ===(G)
      read4At20.advanceToLocus(20 + i * 4 + 3).sequencedBases(0) should ===(T)
    }

    val read4At30 = read4At20.advanceToLocus(20 + 9)
    read4At30.isInsertion should === (true)
    read4At30.sequencedBases should ===("CGTACGTACGT")
  }

  test("Read5: ACGTACGTACGTACG, 5M4=1X5=") {
    // Read5: ACGTACGTACGTACG, 5M4=1X5=, [10; 25[
    //        MMMMM====G=====
    val decadentRead5 = testAdamRecords(4)
    val read5At10 = pileupElementFromRead(decadentRead5, 10)
    assert(read5At10 != null)
    read5At10.advanceToLocus(10).sequencedBases should ===(A)
    read5At10.advanceToLocus(14).sequencedBases should ===(A)
    read5At10.advanceToLocus(18).sequencedBases should ===(A)
    read5At10.advanceToLocus(19).sequencedBases should ===(C)
    read5At10.advanceToLocus(20).sequencedBases should ===(G)
    read5At10.advanceToLocus(21).sequencedBases should ===(T)
    read5At10.advanceToLocus(22).sequencedBases should ===(A)
    read5At10.advanceToLocus(24).sequencedBases should ===(G)
  }

  test("read6: ACGTACGTACGT 4=1N4=4S") {
    // Read6: ACGTACGTACGT 4=1N4=4S
    // one `N` and soft-clipping at the end
    val decadentRead6 = testAdamRecords(5)
    val read6At99 = pileupElementFromRead(decadentRead6, 99)

    assert(read6At99 != null)
    read6At99.advanceToLocus(99).sequencedBases should ===(A)
    read6At99.advanceToLocus(100).sequencedBases should ===(C)
    read6At99.advanceToLocus(101).sequencedBases should ===(G)
    read6At99.advanceToLocus(102).sequencedBases should ===(T)
    read6At99.advanceToLocus(103).sequencedBases should ===("")
    read6At99.advanceToLocus(104).sequencedBases should ===(A)
    read6At99.advanceToLocus(107).sequencedBases should ===(T)
    intercept[AssertionError] {
      read6At99.advanceToLocus(49).sequencedBases
    }
  }

  test("read7: ACGTACGT 4=1N4=4H, one `N` and hard-clipping at the end") {
    val decadentRead7 = testAdamRecords(6)
    val read7At99 = pileupElementFromRead(decadentRead7, 99)
    assert(read7At99 != null)
    read7At99.advanceToLocus(99).sequencedBases should ===(A)
    read7At99.advanceToLocus(100).sequencedBases should ===(C)
    read7At99.advanceToLocus(101).sequencedBases should ===(G)
    read7At99.advanceToLocus(102).sequencedBases should ===(T)
    read7At99.advanceToLocus(103).sequencedBases should ===("")
    read7At99.advanceToLocus(104).sequencedBases should ===(A)
    read7At99.advanceToLocus(107).sequencedBases should ===(T)
    intercept[AssertionError] {
      read7At99.advanceToLocus(49).sequencedBases
    }
  }

  test("create and advance pileup element from RNA read") {
    val start = 229538779
    val rnaRead =
      makeRead(
        sequence = "ACGTAGCCTAGGCCTTCGACACTGGGGGGCTGAGGGAAGGGGCACCTGTC",
        cigar = "7M" + "1000N" + "43M",  // spans 1050 bases of reference
        start = start,
        chr = "1"
      )

    val rnaPileupElement = PileupElement(rnaRead, start, reference.getContig("1"))

    // Second base
    rnaPileupElement.advanceToLocus(start + 1).sequencedBases should ===("C")

    // Third base
    rnaPileupElement.advanceToLocus(start + 2).sequencedBases should ===("G")

    // In intron
    rnaPileupElement.advanceToLocus(start + 100).sequencedBases should ===("")

    // Last base
    rnaPileupElement.advanceToLocus(start + 1049).sequencedBases should ===("C")
  }

  test("create pileup from RNA reads") {
    val rnaReadsPileup = loadPileup(sc, "testrna.sam", locus = 229580594)

    // 94 reads in the testrna.sam
    // 3 reads end at 229580707 and 1 extends further
    rnaReadsPileup.depth should === (94)

    val movedRnaReadsPileup = rnaReadsPileup.atGreaterLocus(229580706, Iterator.empty)
    movedRnaReadsPileup.depth should === (4)

    movedRnaReadsPileup.atGreaterLocus(229580707, Iterator.empty).depth should === (1)
  }

  test("pileup in the middle of a deletion") {
    val reads =
      makeReads(
        ("TCGAAAAGCT", "5M6D5M", 0),
        ("TCGAAAAGCT", "5M6D5M", 0),
        ("TCGAAAAGCT", "5M6D5M", 0)
      )

    val deletionPileup = makePileup(reads, "chr1", 4)
    val deletionAlleles = deletionPileup.distinctAlleles
    deletionAlleles.size should === (1)
    deletionAlleles(0) should === (Allele("ATCGACG", "A"))

    val midDeletionPileup = makePileup(reads, "chr1", 5)
    val midAlleles = midDeletionPileup.distinctAlleles
    midAlleles.size should === (1)
    midAlleles(0) should === (Allele("T", ""))
  }
}

