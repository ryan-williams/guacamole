package org.hammerlab.guacamole.rdd

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class SameElementsRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
  def compareByKey(o: RDD[(K, V)]): Cmp = {
    val joined = rdd.fullOuterJoin(o)
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

  def sameElements(o: RDD[(K, V)]): Boolean = {
    compareByKey(o).isEqual
  }
}

object SameElementsRDD {
  implicit def rddToSameElementsRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): SameElementsRDD[K, V] = new SameElementsRDD(rdd)
}

case class Cmp(equal: Long = 0, notEqual: Long = 0, onlyA: Long = 0, onlyB: Long = 0) {
  def +(o: Cmp): Cmp = Cmp(equal + o.equal, notEqual + o.notEqual, onlyA + o.onlyA, onlyB + o.onlyB)

  def isEqual: Boolean = notEqual == 0 && onlyA == 0 && onlyB == 0
}
