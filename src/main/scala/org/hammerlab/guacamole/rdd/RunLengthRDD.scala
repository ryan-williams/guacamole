package org.hammerlab.guacamole.rdd

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.util.RunLengthIterator

import scala.reflect.ClassTag

class RunLengthRDD[T: ClassTag](rdd: RDD[T]) {
  lazy val runLengthEncode = {
    val encodedPartitions = rdd.mapPartitions(it => new RunLengthIterator(it))

  }
}

object RunLengthRDD {
  implicit def rddToRunLengthRDD[T: ClassTag](rdd: RDD[T]): RunLengthRDD[T] = new RunLengthRDD(rdd)
}
