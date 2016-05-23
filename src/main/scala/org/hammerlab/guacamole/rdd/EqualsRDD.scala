package org.hammerlab.guacamole.rdd

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

import SameElementsRDD._

class EqualsRDD[T: ClassTag](rdd: RDD[T]) {
  def compare(o: RDD[T]): Cmp = {

    val indexedRDD = rdd.zipWithIndex().map(_.swap)
    val indexedOther = o.zipWithIndex().map(_.swap)

    indexedRDD.compareByKey(indexedOther)
  }

  def compareElements(o: RDD[T]): Cmp = {
    for {
      (e, (aO, bO)) <- rdd.map(_ -> null).fullOuterJoin(o.map(_ -> null))
    } yield {
      e -> (aO.isDefined, bO.isDefined)
    }
  }

  def isEqual(o: RDD[T]): Boolean = {
    compare(o).isEqual
  }
}

object EqualsRDD {
  implicit def rddToEqualsRDD[T: ClassTag](rdd: RDD[T]): EqualsRDD[T] = new EqualsRDD(rdd)
}
