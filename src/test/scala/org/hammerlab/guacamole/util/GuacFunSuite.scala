package org.hammerlab.guacamole.util

import org.hammerlab.guacamole.kryo.Registrar
import org.hammerlab.spark.test.suite.KryoSerializerSuite
import org.scalactic.TypeCheckedTripleEquals

class GuacFunSuite
  extends KryoSerializerSuite(classOf[Registrar], referenceTracking = true)
    with SparkSerializerSuite
    with TypeCheckedTripleEquals {
  conf.setAppName("guacamole")
}

