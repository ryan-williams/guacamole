package org.hammerlab.guacamole.pileup

import org.kohsuke.args4j.{Option => Args4JOption}

trait PileupArgs {
  @Args4JOption(
    name = "--pileup-strategy",
    usage = "Strategy to use for constructing pileups: [windows|iterator] (default: 'windows')."
  )
  var pileupStrategy: String = "windows"
}
