package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.loci.partitioning.ApproximatePartitionerArgs
import org.hammerlab.guacamole.variants.Concordance.ConcordanceArgs
import org.hammerlab.guacamole.variants.GenotypeOutputArgs

trait GermlineCallerArgs
  extends GenotypeOutputArgs
    with ReadsArgs
    with ConcordanceArgs
    with ApproximatePartitionerArgs
