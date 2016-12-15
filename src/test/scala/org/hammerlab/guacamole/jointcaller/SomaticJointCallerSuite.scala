package org.hammerlab.guacamole.jointcaller

import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.readsets.ReadSetsUtil
import org.hammerlab.guacamole.commands.SomaticJoint.makeCalls
import org.hammerlab.guacamole.reference.{ ReferenceBroadcast, ReferenceUtil }
import org.hammerlab.guacamole.util.GuacFunSuite
import org.hammerlab.guacamole.util.TestUtil.resourcePath

class SomaticJointCallerSuite
  extends GuacFunSuite
    with ReadSetsUtil
    with ReferenceUtil {

  val cancerWGS1Bams = Vector("normal.bam", "primary.bam", "recurrence.bam").map(
    name => resourcePath("cancer-wgs1/" + name))

  val celsr1BAMs = Vector("normal_0.bam", "tumor_wes_2.bam", "tumor_rna_11.bam").map(
    name => resourcePath("cancer-wes-and-rna-celsr1/" + name))

  val hg19PartialFasta = resourcePath("hg19.partial.fasta")

  def hg19PartialReference =
    ReferenceBroadcast(hg19PartialFasta, sc, partialFasta = true)

  val b37Chromosome22Fasta = resourcePath("chr22.fa.gz")

  def b37Chromosome22Reference =
    ReferenceBroadcast(b37Chromosome22Fasta, sc, partialFasta = false)

  test("force-call a non-variant locus") {
    val inputs = InputCollection(cancerWGS1Bams)
    val (readSets, loci) = makeReadSets(inputs, "chr12:65857040")
    val calls =
      makeCalls(
        sc,
        inputs,
        readSets,
        Parameters.defaults,
        hg19PartialReference,
        loci,
        loci
      )
      .collect

    calls.length === (1)

    val evidences = calls.head.singleAlleleEvidences
    evidences.length === (1)

    val allele = evidences.head.allele
    allele.start === (65857040)
    allele.ref === ("G")
  }

  test("call a somatic deletion") {
    val inputs = InputCollection(cancerWGS1Bams)
    val (readsets, loci) = makeReadSets(inputs, "chr5:82649006-82649009")
    val calls =
      makeCalls(
        sc,
        inputs,
        readsets,
        Parameters.defaults,
        hg19PartialReference,
        loci,
        LociSet(),
        includeFiltered = true
      )
      .collect

    calls.length === (1)
    calls.head.singleAlleleEvidences.length === (1)
    calls.head.singleAlleleEvidences.head.allele.ref === ("TCTTTAGAAA")
    calls.head.singleAlleleEvidences.head.allele.alt === ("T")
  }

  test("call germline variants") {
    val inputs = InputCollection(cancerWGS1Bams.take(1), tissueTypes = Vector("normal"))
    val (readSets, loci) = makeReadSets(inputs, "chr1,chr2,chr3")
    val calls =
      makeCalls(
        sc,
        inputs,
        readSets,
        includeFiltered = true,
        parameters = Parameters.defaults.copy(filterStrandBiasPhred = 20),
        reference = hg19PartialReference,
        loci = loci
      )
      .collect
      .map(call => (call.contigName, call.start) -> call)
      .toMap

    calls(("chr1", 179895860)).singleAlleleEvidences.length === (1)

    val bestAllele = calls(("chr1", 179895860)).bestAllele()

    bestAllele.isSomaticCall === (false)
    bestAllele.isGermlineCall === (true)
    bestAllele.allele.ref === ("T")
    bestAllele.allele.alt === ("C")
    bestAllele.allEvidences.head.annotations.get.strandBias.phredValue === (0)
    bestAllele.allEvidences.head.annotations.get.annotationsFailingFilters === (Seq.empty)
    bestAllele.annotations.get.annotationsFailingFilters === (Seq.empty)


//    TODO: after PR#479 this test fails as the test data no longer contains a germline variant
//    See https://github.com/hammerlab/guacamole/pull/479
//    calls(("chr1", 167190087)).bestAllele.isGermlineCall === (true)
//    calls(("chr1", 167190087)).bestAllele.allele.ref
//    calls(("chr1", 167190087)).bestAllele.allele.alt
//    calls(("chr1", 167190087)).bestAllele.allEvidences.head.annotations.get.strandBias.phredValue > 20 === (true)
//    calls(("chr1", 167190087)).bestAllele.allEvidences.head.annotations.get.strandBias.isFiltered === (true)
//    calls(("chr1", 167190087)).bestAllele.failingFilterNames.contains("STRAND_BIAS") === (true)
  }

  test("don't call variants with N as the reference base") {
    val inputs = InputCollection(cancerWGS1Bams)
    val (readsets, loci) = makeReadSets(inputs, "chr12:65857030-65857080")
    val emptyPartialReference = makeReference(sc, 70000000, ("chr12", 65856930, "N" * 250))

    val calls =
      makeCalls(
        sc,
        inputs,
        readsets,
        Parameters.defaults,
        emptyPartialReference,
        loci,
        loci
      )
      .collect

    calls.length === (0)
  }

  test("call a somatic variant using RNA evidence") {
    val parameters = Parameters.defaults.copy(somaticNegativeLog10VariantPriorWithRnaEvidence = 1)
    val lociStr = "chr22:46931058-46931079"

    val inputsWithRNA = InputCollection(celsr1BAMs, analytes = Vector("dna", "dna", "rna"))

    val callsWithRNA = {
      val (readsets, loci) = makeReadSets(inputsWithRNA, lociStr)
      makeCalls(
        sc,
        inputsWithRNA,
        readsets,
        parameters,
        b37Chromosome22Reference,
        loci
      )
      .collect
      .filter(_.bestAllele.isCall)
    }

    val inputsWithoutRNA = InputCollection(celsr1BAMs.take(2), analytes = Vector("dna", "dna"))

    val callsWithoutRNA = {
      val (readsets, loci) = makeReadSets(inputsWithoutRNA, lociStr)
      makeCalls(
        sc,
        inputsWithoutRNA,
        readsets,
        parameters,
        b37Chromosome22Reference,
        loci
      )
      .collect
      .filter(_.bestAllele.isCall)
    }
    Map(
      "with rna" -> callsWithRNA,
      "without rna" -> callsWithoutRNA
    ).foreach {
      case (description, calls) =>
        withClue("germline variant %s".format(description)) {
          // There should be a germline homozygous call at 22:46931077 in one based, which is 22:46931076 in zero based.
          val filtered46931076 = calls.filter(call => call.start == 46931076 && call.end == 46931077)
          filtered46931076.length === (1)
          filtered46931076.head.bestAllele.isGermlineCall === (true)
          filtered46931076.head.bestAllele.allele.ref === ("G")
          filtered46931076.head.bestAllele.allele.alt === ("C")
          filtered46931076.head.bestAllele.germlineAlleles === ("C", "C")
        }
    }

    // RNA should enable a call G->A call at 22:46931062 in one based, which is 22:46931061 in zero based.
    callsWithoutRNA.exists(call => call.start == 46931061 && call.end == 46931062) === (false)
    val filtered46931061 = callsWithRNA.filter(call => call.start == 46931061 && call.end == 46931062)
    filtered46931061.length === (1)

    val bestAllele = filtered46931061.head.bestAllele

    bestAllele.isSomaticCall === (true)
    bestAllele.allele.ref === ("G")
    bestAllele.allele.alt === ("A")
    bestAllele.tumorDNAPooledEvidence.allelicDepths.toSet === (Set("G" -> 90, "A" -> 2))
    bestAllele.normalDNAPooledEvidence.allelicDepths.toSet === (Set("G" -> 51))
  }
}
