name := "guacamole"
version := "0.1.0-SNAPSHOT"

hadoopVersion := "2.7.2"

addSparkDeps

deps ++= Seq(
  libs.value('adam_core),
  libs.value('args4j),
  libs.value('args4s),
  libs.value('bdg_formats),
  libs.value('bdg_utils_cli),
  "org.apache.commons" % "commons-math3" % "3.6.1",
  libs.value('hadoop_bam),
  libs.value('htsjdk),
  libs.value('loci),
  libs.value('magic_rdds),
  libs.value('reference),
  "org.scalautils" %% "scalautils" % "2.1.5",
  //"org.scalaz" %% "scalaz-core" % "7.2.8",
  libs.value('slf4j),
  libs.value('spark_commands),
  libs.value('spark_util),
  libs.value('spire)
)

providedDeps ++= Seq(
  libs.value('mllib)
)

testDeps ++= Seq(
  libs.value('spark_testing_base) exclude("org.apache.hadoop", "hadoop-client")
)

compileAndTestDeps ++= Seq(
  libs.value('reads),
  libs.value('readsets)
)

takeFirstLog4JProperties

mainClass := Some("org.hammerlab.guacamole.Main")

shadedDeps += "org.scalanlp" %% "breeze" % "0.12"
shadeRenames += "breeze.**" -> "org.hammerlab.breeze.@1"

//publishThinShadedJar
