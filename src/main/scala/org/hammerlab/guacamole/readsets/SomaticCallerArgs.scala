package org.hammerlab.guacamole.readsets

import org.hammerlab.guacamole.loci.partitioning.AllLociPartitionerArgs
import org.hammerlab.guacamole.variants.GenotypeOutputArgs

trait SomaticCallerArgs
  extends GenotypeOutputArgs
    with TumorNormalReadsArgs
    with AllLociPartitionerArgs
