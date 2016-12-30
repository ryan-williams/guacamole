package org.hammerlab.guacamole

import org.hammerlab.genomics.bases.Bases

package object jointcaller {
  implicit class AllelicDepths(val map: Map[Bases, Int]) extends AnyVal {
    def take(num: Int): AllelicDepths =
      map
        .toVector
        .sortBy(-_._2)
        .take(num)
        .toMap
  }

  object AllelicDepths {
    implicit def unwrapAllelicDepths(allelicDepths: AllelicDepths): Map[Bases, Int] = allelicDepths.map
  }
}
