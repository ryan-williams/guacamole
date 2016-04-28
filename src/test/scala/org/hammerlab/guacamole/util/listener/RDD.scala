package org.hammerlab.guacamole.util.listener

import org.hammerlab.guacamole.util.listener.TestSparkListener.RddId

case class RDD(id: RddId, name: String, numPartitions: Int, callSite: String, parents: Seq[RddId])
