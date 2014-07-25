package org.bdgenomics.guacamole.data

import org.bdgenomics.guacamole.callers.Genotype
import scala.collection.JavaConversions
import org.bdgenomics.guacamole.Bases
import org.bdgenomics.adam.util.PhredUtils
import org.bdgenomics.formats.avro.{ ADAMVariant, ADAMContig, ADAMGenotype }

object CalledGenotype {

  implicit def calledGenotypeToADAMGenotype(calledGenotype: CalledGenotype): Seq[ADAMGenotype] = {
    val genotypeAlleles = JavaConversions.seqAsJavaList(calledGenotype.alleles.getGenotypeAlleles(Bases.baseToString(calledGenotype.referenceBase)))
    calledGenotype.alleles.getNonReferenceAlleles(Bases.baseToString(calledGenotype.referenceBase)).map(
      variantAllele => {
        val variant = ADAMVariant.newBuilder
          .setPosition(calledGenotype.locus)
          .setReferenceAllele(Bases.baseToString(calledGenotype.referenceBase))
          .setVariantAllele(variantAllele)
          .setContig(ADAMContig.newBuilder.setContigName(calledGenotype.referenceName).build)
          .build
        ADAMGenotype.newBuilder
          .setAlleles(genotypeAlleles)
          .setSampleId(calledGenotype.sampleName.toCharArray)
          .setGenotypeQuality(calledGenotype.evidence.phredScaledLikelihood)
          .setReadDepth(calledGenotype.evidence.readDepth)
          .setExpectedAlleleDosage(calledGenotype.evidence.alternateReadDepth.toFloat / calledGenotype.evidence.readDepth)
          .setAlternateReadDepth(calledGenotype.evidence.alternateReadDepth)
          .setVariant(variant)
          .build
      })
  }
}

case class CalledGenotype(sampleName: String,
                          referenceName: String,
                          locus: Long,
                          referenceBase: Byte,
                          alternateBase: String,
                          alleles: Genotype,
                          evidence: GenotypeEvidence)

case class GenotypeEvidence(likelihood: Double,
                            readDepth: Int,
                            alternateReadDepth: Int,
                            forwardDepth: Int,
                            alternateForwardDepth: Int) {

  lazy val phredScaledLikelihood = PhredUtils.successProbabilityToPhred(likelihood)

}
