package org.bdgenomics.guacamole.callers

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.cli.Args4j
import org.bdgenomics.guacamole.Common.Arguments.{ Base, Output, Reads }
import org.bdgenomics.guacamole.filters.GenotypeFilter.GenotypeFilterArguments
import org.bdgenomics.guacamole.reads.{ MappedRead, Read }
import org.bdgenomics.guacamole.variants.{ AlleleConversions, Breakpoint, CalledAllele }
import org.bdgenomics.guacamole.{ Command, Common, DelayedMessages, DistributedUtil }
import org.kohsuke.args4j.{ Option => Opt }

object PairedEndAlignmentSVCaller extends Command with Serializable with Logging {

  override val name = "alignment-sv"
  override val description = "call structural variants using a simple alignment rules"

  private class Arguments extends Base with Output with Reads with GenotypeFilterArguments with DistributedUtil.Arguments {

    @Opt(name = "-threshold", usage = "threshold")
    var threshold: Int = 0

  }

  def discoverBreakpoints(lastBreakpointOpt: Option[Breakpoint],
                          readsAtLocus: Seq[MappedRead],
                          threshold: Double,
                          minDepth: Int): (Option[Breakpoint], Seq[CalledAllele]) = {

    val readsSupportingBreakpoint = readsAtLocus.filter(r => r.inDuplicatedRegion || r.inInvertedRegion)
    val breakpointRatio = readsSupportingBreakpoint.size.toFloat / readsAtLocus.size
    if (breakpointRatio > threshold && readsAtLocus.size > minDepth) {
      val newBreakpoint = Breakpoint(readsSupportingBreakpoint)
      lastBreakpointOpt match {
        case Some(lastBreakpoint) => {
          if (newBreakpoint.overlaps(lastBreakpoint)) {
            (Some(newBreakpoint.merge(lastBreakpoint)), Seq.empty)
          } else {
            (Some(newBreakpoint), Seq(lastBreakpoint.callAllele))
          }
        }
        case None => (Some(newBreakpoint), Seq.empty)
      }
    } else {
      (lastBreakpointOpt, Seq.empty)
    }

  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(appName = Some(name))

    val filters = Read.InputFilters(mapped = true, nonDuplicate = true)
    val readSet = Common.loadReadsFromArguments(args, sc, filters = filters)
    val mappedReads = readSet.mappedReads

    //mappedReads.persist(StorageLevel.MEMORY_ONLY_SER)

    Common.progress("Loaded %,d mapped non-duplicate MdTag-containing reads into %,d partitions.".format(
      mappedReads.count, mappedReads.partitions.length))

    val loci = Common.loci(args, readSet)
    val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(
      args,
      loci,
      mappedReads
    )

    val threshold = args.threshold / 100.0
    val minDepth = args.minReadDepth
    val genotypes = DistributedUtil.windowTaskFlatMapMultipleRDDs(
      Seq(mappedReads),
      lociPartitions,
      0L,
      (task, taskLoci, taskRegionsSeq: Seq[Iterator[MappedRead]]) => {
        DistributedUtil.collectByContig[MappedRead, CalledAllele](
          taskRegionsSeq,
          taskLoci,
          true,
          0L,
          (windowsIterator) => {
            var lastBreakpoint: Option[Breakpoint] = None
            val variants = windowsIterator.flatMap(windows => {
              val window = windows(0)
              val (currentBreakpoint, currentVariants) =
                discoverBreakpoints(
                  lastBreakpoint,
                  window.currentRegions(),
                  threshold,
                  minDepth
                )
              lastBreakpoint = currentBreakpoint
              currentVariants
            })
            variants ++ lastBreakpoint.map(_.callAllele)
          }
        )
      }
    )

    genotypes.persist(StorageLevel.MEMORY_ONLY_SER)

    Common.progress("Computed %,d genotypes".format(genotypes.count))

    val filteredGenotypes = genotypes.flatMap(AlleleConversions.calledAlleleToADAMGenotype)
    Common.writeVariantsFromArguments(args, filteredGenotypes)
    DelayedMessages.default.print()

  }

}

