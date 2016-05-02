package org.hammerlab.guacamole

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.lang.{Long => JLong}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.google.common.collect.{TreeRangeMap, Range => JRange}
import org.apache.spark.SparkContext
import org.apache.spark.serializer.KryoRegistrator

trait CanRun {
  def main(args: Array[String]): Unit = {
    run(SparkContext.getOrCreate())
  }

  def run(sc: SparkContext): Unit
}

object NS extends CanRun {
  def run(sc: SparkContext): Unit = {
    val o = new NotSerializable(100)
    val bc = sc.broadcast(o)  // Fails: kryo NS, all.
    sc.parallelize(1 to 10, 10).map(i => i * bc.value.n).collect()
  }
}

object JNK extends CanRun {
  def run(sc: SparkContext): Unit = {
    val o = new JavaNotKryo(100)
    val bc = sc.broadcast(o)  // Fails: kryo JNK, all.
    sc.parallelize(1 to 10, 10).map(i => i * bc.value.n).collect()
  }
}

object KNJ extends CanRun {
  def run(sc: SparkContext): Unit = {
    val o = new KryoNotJava(100)
    val bc = sc.broadcast(o)
    sc.parallelize(1 to 10, 10).map(i => i * bc.value.n).collect()  // Succeeds (submit); fails in shell; java KNJ.
  }
}

object JCAK extends CanRun {
  def run(sc: SparkContext): Unit = {
    val o = new JavaCustomAndKryo(new NotSerializable(100))
    val bc = sc.broadcast(o)  // Fails: kryo NS, all.
    sc.parallelize(1 to 10, 10).map(i => i * bc.value.ns.n).collect()
  }
}

object KCAJ extends CanRun {
  def run(sc: SparkContext): Unit = {
    val o = new KryoCustomAndJava(new NotSerializable(100))
    val bc = sc.broadcast(o)
    sc.parallelize(1 to 10, 10).map(i => i * bc.value.ns.n).collect()  // Succeeds (submit); fails in shell; java NS
  }
}

object BS extends CanRun {
  def run(sc: SparkContext): Unit = {
    val o = new BothSerializable(100)
    val bc = sc.broadcast(o)
    sc.parallelize(1 to 10, 10).map(i => i * bc.value.n).collect()  // Succeeds (all).
  }
}

object BC extends CanRun {
  def run(sc: SparkContext): Unit = {
    val o = new BothCustom(new NotSerializable(100))
    val bc = sc.broadcast(o)
    sc.parallelize(1 to 10, 10).map(i => i * bc.value.ns.n).collect()  // Succeeds (all).
  }
}

class TestRegistrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {

    kryo.register(classOf[Range])
    kryo.register(classOf[Array[Int]])

    kryo.register(classOf[KryoNotJava])
    kryo.register(classOf[KryoCustom], new KryoCustomSerializer)
    kryo.register(classOf[BothSerializable])
    kryo.register(classOf[KryoCustomAndJava], new KryoCustomAndJavaSerializer)
    kryo.register(classOf[JavaCustomAndKryo])
    kryo.register(classOf[BothCustom], new BothCustomSerializer)
  }
}

class NotSerializable(var n: Int)

class KryoNotJava(val n: Int)

class JavaNotKryo(val n: Int) extends Serializable

class BothSerializable(val n: Int) extends Serializable

class JavaCustom(var ns: NotSerializable) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeInt(ns.n)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    ns = new NotSerializable(in.readInt())
  }
}

class KryoCustom(val ns: NotSerializable)

class KryoCustomSerializer extends Serializer[KryoCustom] {
  override def write(kryo: Kryo, output: Output, o: KryoCustom): Unit = {
    output.writeInt(o.ns.n)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[KryoCustom]): KryoCustom = {
    new KryoCustom(new NotSerializable(input.readInt()))
  }
}


class KryoCustomAndJava(var ns: NotSerializable) extends Serializable

class KryoCustomAndJavaSerializer extends Serializer[KryoCustomAndJava] {
  override def write(kryo: Kryo, output: Output, o: KryoCustomAndJava): Unit = {
    output.writeInt(o.ns.n)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[KryoCustomAndJava]): KryoCustomAndJava = {
    new KryoCustomAndJava(new NotSerializable(input.readInt()))
  }
}

class JavaCustomAndKryo(var ns: NotSerializable) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeInt(ns.n)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    ns = new NotSerializable(in.readInt())
  }
}

class BothCustom(var ns: NotSerializable) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeInt(ns.n)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    ns = new NotSerializable(in.readInt())
  }
}

class BothCustomSerializer extends Serializer[BothCustom] {
  override def write(kryo: Kryo, output: Output, o: BothCustom): Unit = {
    output.writeInt(o.ns.n)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[BothCustom]): BothCustom = {
    new BothCustom(new NotSerializable(input.readInt()))
  }
}



import scala.collection.JavaConversions._

object Test {
  def main(args: Array[String]): Unit = {
    run(SparkContext.getOrCreate())
  }

  def run(sc: SparkContext): Unit = {
    val map = TreeRangeMap.create[JLong, Int]()
    map.put(JRange.closedOpen(100L, 200L), 123)
    val mapBroadcast = sc.broadcast(map)
    println(
      sc.parallelize(1 to 1000)
      .map(i => i + mapBroadcast.value.getEntry(100L).getValue)
      .take(10)
      .mkString("\n")
    )
  }
}

class TreeRangeMapSerializer[K <: Comparable[_], V] extends Serializer[TreeRangeMap[K, V]] {
  override def write(kryo: Kryo, output: Output, o: TreeRangeMap[K, V]): Unit = {
    val ranges = o.asMapOfRanges().toArray.map(t => (t._1.lowerEndpoint(), t._1.upperEndpoint(), t._2))
    kryo.writeClassAndObject(output, ranges)
  }

  override def read(kryo: Kryo, input: Input, cls: Class[TreeRangeMap[K, V]]): TreeRangeMap[K, V] = {
    val ranges: Array[(K, K, V)] = kryo.readClassAndObject(input).asInstanceOf[Array[(K, K, V)]]
    val map = TreeRangeMap.create[K, V]()
    for {
      (start, end, value) <- ranges
      range = JRange.closedOpen[K](start, end)
    } {
      map.put(range, value)
    }
    map
  }
}

