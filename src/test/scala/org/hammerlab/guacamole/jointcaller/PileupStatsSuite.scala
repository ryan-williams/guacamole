package org.hammerlab.guacamole.jointcaller

import org.hammerlab.genomics.bases.Base.{ A, C, G, N, T }
import org.hammerlab.genomics.bases.{ Base, Bases }
import org.hammerlab.genomics.bases.Bases._
import org.hammerlab.genomics.reads.ReadsUtil
import org.hammerlab.guacamole.jointcaller.pileup_summarization.{ AlleleMixture, PileupStats }
import org.hammerlab.guacamole.pileup.{ Util ⇒ PileupUtil }
import org.hammerlab.guacamole.reference.{ ReferenceBroadcast, ReferenceUtil }
import org.hammerlab.guacamole.util.GuacFunSuite
import org.hammerlab.guacamole.util.TestUtil.resourcePath
import org.hammerlab.test.implicits.convertMap

class PileupStatsSuite
  extends GuacFunSuite
    with ReadsUtil
    with PileupUtil
    with ReferenceUtil {

  implicit def makeAlleleMixture(strsMap: Map[String, Double]): AlleleMixture =
    for {
      (alleleStr, weight) ← strsMap
    } yield
      (alleleStr: Bases) → weight

  implicit def makeBasesWeight(t: (String, Double)): (Bases, Double) = (t._1: Bases, t._2)

  val cancerWGS1Bams = Seq("normal.bam", "primary.bam", "recurrence.bam").map(
    name => resourcePath("cancer-wgs1/" + name))

  val partialFasta = resourcePath("hg19.partial.fasta")
  def partialReference = {
    ReferenceBroadcast(partialFasta, sc, partialFasta = true)
  }

  val refString = "NTCGATCGA"
  override lazy val reference = makeReference(sc, "chr1", 0, refString)

  implicit def makeSimpleMixture(m: Map[Base, Double]): AlleleMixture = m.map(t => Bases(t._1) → t._2)
  implicit def makeAllelicDepthsFromBaseMap(m: Map[Base, Int]): AllelicDepths = m.map(t => Bases(t._1) → t._2)
  implicit def makeAllelicDepthsFromStringMap(m: Map[String, Int]): AllelicDepths = m.map(t => (t._1: Bases) → t._2)

  implicit def s2v[T](s: Seq[T]): Vector[T] = s.toVector

  implicit val m2m = convertMap[String, Int, Bases, Int] _

  test("pileupstats likelihood computation") {

    val reads =
      Seq(
        makeRead(   "TCGATCGA",     "8M", qualityScores = Seq.fill( 8)(10)),
        makeRead(   "TCGATCGA",     "8M", qualityScores = Seq.fill( 8)(20)),
        makeRead(   "TCGCTCGA",     "8M", qualityScores = Seq.fill( 8)(50)),
        makeRead(   "TCGCTCGA",     "8M", qualityScores = Seq.fill( 8)(50)),
        makeRead(   "TCGCTCGA",     "8M", qualityScores = Seq.fill( 8)(50)),
        makeRead("TCGACCCTCGA", "4M3I4M", qualityScores = Seq.fill(11)(30))
      )

    val pileups = (1 until refString.length).map(locus => makePileup(reads, "chr1", locus))

    val stats1 = PileupStats.apply(pileups(1).elements, G)
    stats1.totalDepthIncludingReadsContributingNoAlleles should === (6)
    stats1.allelicDepths should === (Map(G -> 6))
    stats1.nonRefAlleles should === (Seq.empty)
    stats1.topAlt should === (N)
    stats1.logLikelihoodPileup(Map(G -> 1.0)) should be > stats1.logLikelihoodPileup(Map(G → .99, C -> .01))
    //stats1.logLikelihoodPileup(Map(G -> 1.0)) should be > stats1.logLikelihoodPileup(Map(G -> .99, C -> .01))
    stats1.logLikelihoodPileup(Map(T -> 1.0)) should be < stats1.logLikelihoodPileup(Map(G -> .99, C -> .01))

    val stats2 = PileupStats.apply(pileups(2).elements, A)
    stats2.allelicDepths should === (Map("A" -> 2, "C" -> 3, "ACCC" -> 1))
    stats2.nonRefAlleles should === (Seq[Bases](C, "ACCC"))
    assert(stats2.logLikelihoodPileup(Map(A -> 0.5, C -> 0.5)) > stats2.logLikelihoodPileup(Map(A -> 1.0)))

    // True because of the higher base qualities on the C allele:
    assert(stats2.logLikelihoodPileup(Map(C -> 1.0)) > stats2.logLikelihoodPileup(Map(A -> 1.0)))

    val stats3 = PileupStats.apply(pileups(3).elements, T)
    stats3.totalDepthIncludingReadsContributingNoAlleles should === (6)
    stats3.allelicDepths should === (Map(T -> 2)) // reads with an SNV at position 4 don't count
  }
}
