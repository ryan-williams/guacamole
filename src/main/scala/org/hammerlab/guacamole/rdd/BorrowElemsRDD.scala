package org.hammerlab.guacamole.rdd

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class BorrowElemsRDD[T: ClassTag](rdd: RDD[T]) {

}

object BorrowElemsRDD {
  implicit def rddToBorrowElemsRDD[T: ClassTag](rdd: RDD[T]): BorrowElemsRDD[T] = new BorrowElemsRDD(rdd)
}
