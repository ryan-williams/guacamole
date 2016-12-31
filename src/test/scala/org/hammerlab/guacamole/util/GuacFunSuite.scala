package org.hammerlab.guacamole.util

import org.hammerlab.genomics.bases.BasesUtil
import org.hammerlab.genomics.reference.test.LocusUtil
import org.hammerlab.guacamole.kryo.Registrar
import org.hammerlab.spark.test.suite.KryoSerializerSuite

class GuacFunSuite
  extends KryoSerializerSuite(classOf[Registrar], referenceTracking = true)
    with SparkSerializerSuite
    with BasesUtil
    with LocusUtil {

  conf.setAppName("guacamole")
}

