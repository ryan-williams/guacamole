package org.hammerlab.guacamole.util

import org.hammerlab.guacamole.kryo.Registrar
import org.hammerlab.spark.test.suite.KryoSerializerSuite
import org.hammerlab.genomics.reference.test.LocusUtil

class GuacFunSuite
  extends KryoSerializerSuite(classOf[Registrar], referenceTracking = true)
    with SparkSerializerSuite
    with LocusUtil {
  conf.setAppName("guacamole")
}

