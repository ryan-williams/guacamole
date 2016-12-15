package org.hammerlab.guacamole.jointcaller

import org.hammerlab.guacamole.pileup.{ Util ⇒ PileupUtil }
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.util.GuacFunSuite
import org.hammerlab.guacamole.util.TestUtil.resourcePath

class AlleleAtLocusSuite
  extends GuacFunSuite
    with PileupUtil {

  val celsr1BAMs =
    Vector("normal_0.bam", "tumor_wes_2.bam", "tumor_rna_11.bam")
      .map(name => s"cancer-wes-and-rna-celsr1/$name")

  val b37Chromosome22Fasta = resourcePath("chr22.fa.gz")

  override lazy val reference =
    ReferenceBroadcast(b37Chromosome22Fasta, sc, partialFasta = false)

  test("AlleleAtLocus.variantAlleles for low vaf variant allele") {
    val inputs = InputCollection(celsr1BAMs, analytes = Vector("dna", "dna", "rna"))

    val pileups =
      (inputs.normalDNA ++ inputs.tumorDNA).map(
        input =>
          loadPileup(sc, input.path, 46931060, Some("chr22"))
      )

    val possibleAlleles =
      AlleleAtLocus.variantAlleles(
        pileups,
        anyAlleleMinSupportingReads = 2,
        anyAlleleMinSupportingPercent = 2,
        maxAlleles = Some(5),
        atLeastOneAllele = false,
        onlyStandardBases = true
      )

    possibleAlleles === (Seq(AlleleAtLocus("chr22", 46931061, "G", "A")))
  }
}

//class AALTest
//  extends KryoSerializerSuite
//    with SparkSerializerSuite {
//
//  kryoRegister(classOf[MappedRead], new MappedReadSerializer)
//
//  kryoRegister(
//    //classOf[MappedRead] → new MappedReadSerializer,
//    classOf[AlleleAtLocus],
//    classOf[mutable.WrappedArray.ofByte],
////    classOf[mutable.ArraySeq[_]],
//    classOf[Cigar],
//    classOf[Array[Object]]
//  )
//
//  test("serde") {
//    val aal = AlleleAtLocus("chr22", 46931061, "G", "A")
//    deserialize[AlleleAtLocus](serialize(aal)) === (aal)
//  }
//
//  test("read") {
//    val read = MappedRead(
//      "read1",
//      "TCGACCCTCGA",
//      Array[Byte]((10 to 20).map(_.toByte): _*),
//      isDuplicate = true,
//      "chr5",
//      50,
//      325352323,
//      TextCigarCodec.decode(""),
//      failedVendorQualityChecks = false,
//      isPositiveStrand = true,
//      isPaired = true
//    )
//
//    deserialize[MappedRead](serialize(read)) === (read)
//  }
//}
