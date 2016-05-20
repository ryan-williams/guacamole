package org.hammerlab.guacamole.rdd

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class Cmp(equal: Long = 0, notEqual: Long = 0, onlyA: Long = 0, onlyB: Long = 0) {
  def +(o: Cmp): Cmp = Cmp(equal + o.equal, notEqual + o.notEqual, onlyA + o.onlyA, onlyB + o.onlyB)

  def isEqual: Boolean = notEqual == 0 && onlyA == 0 && onlyB == 0
}

class EqualsRDD[T: ClassTag](rdd: RDD[T]) {
  def compare(o: RDD[T]): Cmp = {

    val indexedRDD = rdd.zipWithIndex().map(_.swap)
    val indexedOther = o.zipWithIndex().map(_.swap)

    val joined = indexedRDD.fullOuterJoin(indexedOther)
    val cmp =
      (for {
        (idx, (o1, o2)) <- joined
      } yield {
        (o1, o2) match {
          case (Some(e1), Some(e2)) =>
            if (e1 == e2)
              Cmp(equal = 1)
            else
              Cmp(notEqual = 1)
          case (Some(e1), _) => Cmp(onlyA = 1)
          case _ => Cmp(onlyB = 1)
        }
      }).reduce(_ + _)

    cmp
  }

  def isEqual(o: RDD[T]): Boolean = {
    compare(o).isEqual
  }
}

object EqualsRDD {
  implicit def rddToEqualsRDD[T: ClassTag](rdd: RDD[T]): EqualsRDD[T] = new EqualsRDD(rdd)
}
