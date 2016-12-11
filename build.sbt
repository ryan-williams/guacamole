name := "guacamole"
version := "0.1.0-SNAPSHOT"

sparkVersion := "1.6.1"
hadoopVersion := "2.7.2"

addSparkDeps

providedDeps ++= Seq(
  libraries.value('mllib)
)

libraryDependencies ++= Seq(
  libraries.value('adam_core),
  libraries.value('args4j),
  libraries.value('args4s),
  libraries.value('bdg_formats),
  libraries.value('bdg_utils_cli),
  libraries.value('commons_math),
  libraries.value('hadoop_bam),
  libraries.value('htsjdk),
  libraries.value('loci),
  libraries.value('magic_rdds),
  libraries.value('reads),
  libraries.value('readsets),
  libraries.value('reference),
  libraries.value('slf4j),
  libraries.value('spark_commands),
  libraries.value('spark_util),
  libraries.value('spire)
)

testDeps ++= Seq(
  libraries.value('spark_testing_base)/*,
  libraries.value('hadoop)*/
)

testJarTestDeps ++= Seq(
  libraries.value('reads),
  libraries.value('readsets)
)

assemblyMergeStrategy in assembly := {
  // Two org.bdgenomics deps include the same log4j.properties.
  case PathList("log4j.properties") => MergeStrategy.first
  case x => (assemblyMergeStrategy in assembly).value(x)
}

mainClass := Some("org.hammerlab.guacamole.Main")

shadedDeps += "org.scalanlp" %% "breeze" % "0.12"
shadeRenames += "breeze.**" -> "org.hammerlab.breeze.@1"

//publishThinShadedJar
