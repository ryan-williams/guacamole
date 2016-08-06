package org.hammerlab.guacamole.readsets.iterator

import org.hammerlab.guacamole.loci.iterator.{MergeLociIterator, SkippableLociIterator}
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.{ContigIterator, HasContig, Locus, Position}

object FilterAndRequireLoci {
  def apply[C <: HasContig, T](elems: Iterator[C],
                               loci: LociSet,
                               requiredLoci: LociSet,
                               makeContigIterator: ContigIterator[C] => SkippableLociIterator[(Locus, T)],
                               empty: T): Iterator[(Position, T)] =
    for {
      // For each contig…
      (contigName, contigReads) <- ContigsIterator(elems.buffered)

      // 1. Locus-keyed objects on this contig.
      contigObjs = makeContigIterator(contigReads)

      // 2. Restrict output to these loci.
      contigLoci = loci.onContig(contigName).iterator

      // 3. Restrict from 1., to loci from 2.
      filteredObjs = contigObjs.intersect(contigLoci)

      // 4. Loci that we must emit, paired with empty values; [[UnionLociIterator]] will override these empty values
      // with any non-empty values found in [[contigObjs]].
      emptyRequiredLoci = requiredLoci.onContig(contigName).iterator.map(_ -> empty).buffered

      // 5. Merge filtered objects from 3. with empty-valued required-loci from 4.
      mergedObjs = new MergeLociIterator(filteredObjs, emptyRequiredLoci)

      (locus, obj) <- mergedObjs
    } yield
      Position(contigName, locus) -> obj
}
