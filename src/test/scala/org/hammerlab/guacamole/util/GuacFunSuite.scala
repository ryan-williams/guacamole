package org.hammerlab.guacamole.util

import org.hammerlab.guacamole.kryo.Registrar
import org.hammerlab.spark.test.suite.KryoSerializerSuite
import org.hammerlab.genomics.reference.test.{ BasesUtil, LocusUtil }
import org.scalactic.ConversionCheckedTripleEquals

class GuacFunSuite
  extends KryoSerializerSuite(classOf[Registrar], referenceTracking = true)
    with SparkSerializerSuite
    with ConversionCheckedTripleEquals
    with Implicits
    with BasesUtil
    with LocusUtil {

  conf.setAppName("guacamole")
}

